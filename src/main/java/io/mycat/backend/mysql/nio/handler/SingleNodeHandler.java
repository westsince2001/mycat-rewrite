/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese 
 * opensource volunteers. you can redistribute it and/or modify it under the 
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 * 
 * Any questions about this component can be directed to it's project Web address 
 * https://code.google.com/p/opencloudb/.
 *
 */
package io.mycat.backend.mysql.nio.handler;

import customer.rewrite.mycat.config.CustomerConfig;
import customer.rewrite.mycat.util.BytesHexStrTranslate;
import customer.rewrite.mycat.util.EncrptUtil;
import customer.rewrite.mycat.vo.FieldPacketWrapper;
import io.mycat.util.ByteBufferUtil;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.google.common.base.Strings;

import io.mycat.MycatServer;
import io.mycat.backend.BackendConnection;
import io.mycat.backend.datasource.PhysicalDBNode;
import io.mycat.backend.mysql.LoadDataUtil;
import io.mycat.config.ErrorCode;
import io.mycat.config.MycatConfig;
import io.mycat.config.model.SchemaConfig;
import io.mycat.net.mysql.BinaryRowDataPacket;
import io.mycat.net.mysql.ErrorPacket;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.OkPacket;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.route.RouteResultset;
import io.mycat.route.RouteResultsetNode;
import io.mycat.server.NonBlockingSession;
import io.mycat.server.ServerConnection;
import io.mycat.server.parser.ServerParse;
import io.mycat.server.parser.ServerParseShow;
import io.mycat.server.response.ShowFullTables;
import io.mycat.server.response.ShowTables;
import io.mycat.statistic.stat.QueryResult;
import io.mycat.statistic.stat.QueryResultDispatcher;
import io.mycat.util.ResultSetUtil;
import io.mycat.util.StringUtil;

import org.slf4j.Logger; import org.slf4j.LoggerFactory;
/**
 * @author mycat
 */
public class SingleNodeHandler implements ResponseHandler, Terminatable, LoadDataResponseHandler {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(SingleNodeHandler.class);
	
	private final RouteResultsetNode node;
	private final RouteResultset rrs;
	private final NonBlockingSession session;
	
	// only one thread access at one time no need lock
	private volatile byte packetId;
	private volatile ByteBuffer buffer;
	private volatile boolean isRunning;
	private Runnable terminateCallBack;
	private long startTime;
	private long netInBytes;
	private long netOutBytes;
	private long selectRows;
	private long affectedRows;
	
	private boolean prepared;
	private int fieldCount;
	private List<FieldPacket> fieldPackets = new ArrayList<FieldPacket>();

    private volatile boolean isDefaultNodeShowTable;
    private volatile boolean isDefaultNodeShowFullTable;
    private  Set<String> shardingTablesSet;
	private byte[] header = null;
	private List<byte[]> fields = null;

	/***
	 * 数列原始的下标记录
	 */
	private List<Integer> columnSourceIndexList = new ArrayList<Integer>();
	/***
	 * 需要跳过字段下标。
	 */
	private List<Integer> columnSkipIndexList = new ArrayList<Integer>();
	/***
	 * 需要内容改写的下标列
	 */
	private List<Integer> columnFilterIndexList = new ArrayList<Integer>();

	public SingleNodeHandler(RouteResultset rrs, NonBlockingSession session) {
		this.rrs = rrs;
		this.node = rrs.getNodes()[0];
		
		if (node == null) {
			throw new IllegalArgumentException("routeNode is null!");
		}
		
		if (session == null) {
			throw new IllegalArgumentException("session is null!");
		}
		
		this.session = session;
		ServerConnection source = session.getSource();
		String schema = source.getSchema();
		if (schema != null && ServerParse.SHOW == rrs.getSqlType()) {
			SchemaConfig schemaConfig = MycatServer.getInstance().getConfig().getSchemas().get(schema);
			int type = ServerParseShow.tableCheck(rrs.getStatement(), 0);
			isDefaultNodeShowTable = (ServerParseShow.TABLES == type && !Strings.isNullOrEmpty(schemaConfig.getDataNode()));
			isDefaultNodeShowFullTable = (ServerParseShow.FULLTABLES == type && !Strings.isNullOrEmpty(schemaConfig.getDataNode()));
			if (isDefaultNodeShowTable) {
				shardingTablesSet = ShowTables.getTableSet(source, rrs.getStatement());
				
			} else if (isDefaultNodeShowFullTable) {
				shardingTablesSet = ShowFullTables.getTableSet(source, rrs.getStatement());
			}
		}
        
		if ( rrs != null && rrs.getStatement() != null) {
			netInBytes += rrs.getStatement().getBytes().length;
		}
        
	}

	@Override
	public void terminate(Runnable callback) {
		boolean zeroReached = false;

		if (isRunning) {
			terminateCallBack = callback;
		} else {
			zeroReached = true;
		}

		if (zeroReached) {
			callback.run();
		}
	}

	private void endRunning() {
		Runnable callback = null;
		if (isRunning) {
			isRunning = false;
			callback = terminateCallBack;
			terminateCallBack = null;
		}

		if (callback != null) {
			callback.run();
		}
	}

	private void recycleResources() {

		ByteBuffer buf = buffer;
		if (buf != null) {
			session.getSource().recycle(buffer);
			buffer = null;
		}
	}

	public void execute() throws Exception {

		LOGGER.info("===========execute========");
		startTime=System.currentTimeMillis();
		ServerConnection sc = session.getSource();
		this.isRunning = true;
		this.packetId = 0;
		final BackendConnection conn = session.getTarget(node);
		LOGGER.debug("rrs.getRunOnSlave() " + rrs.getRunOnSlave());
		node.setRunOnSlave(rrs.getRunOnSlave());	// 实现 master/slave注解
		LOGGER.debug("node.getRunOnSlave() " + node.getRunOnSlave());
		 
		if (session.tryExistsCon(conn, node)) {
			_execute(conn);
		} else {
			// create new connection

			MycatConfig conf = MycatServer.getInstance().getConfig();
						
			LOGGER.debug("node.getRunOnSlave() " + node.getRunOnSlave());
			node.setRunOnSlave(rrs.getRunOnSlave());	// 实现 master/slave注解
			LOGGER.debug("node.getRunOnSlave() " + node.getRunOnSlave());
			 		
			PhysicalDBNode dn = conf.getDataNodes().get(node.getName());
			dn.getConnection(dn.getDatabase(), sc.isAutocommit(), node, this, node);
		}

	}

	@Override
	public void connectionAcquired(final BackendConnection conn) {
		session.bindConnection(node, conn);
		_execute(conn);

	}

	private void _execute(BackendConnection conn) {
		if (session.closed()) {
			endRunning();
			session.clearResources(true);
			return;
		}
		conn.setResponseHandler(this);
		try {
			conn.execute(node, session.getSource(), session.getSource()
					.isAutocommit());
		} catch (Exception e1) {
			executeException(conn, e1);
			return;
		}
	}

	private void executeException(BackendConnection c, Exception e) {
		ErrorPacket err = new ErrorPacket();
		err.packetId = ++packetId;
		err.errno = ErrorCode.ERR_FOUND_EXCEPION;
		err.message = StringUtil.encode(e.toString(), session.getSource().getCharset());

		this.backConnectionErr(err, c);
	}

	@Override
	public void connectionError(Throwable e, BackendConnection conn) {

		endRunning();
		ErrorPacket err = new ErrorPacket();
		err.packetId = ++packetId;
		err.errno = ErrorCode.ER_NEW_ABORTING_CONNECTION;
		err.message = StringUtil.encode(e.getMessage(), session.getSource().getCharset());
		
		ServerConnection source = session.getSource();
		source.write(err.write(allocBuffer(), source, true));
	}

	@Override
	public void errorResponse(byte[] data, BackendConnection conn) {
		ErrorPacket err = new ErrorPacket();
		err.read(data);
		err.packetId = ++packetId;
		backConnectionErr(err, conn);
	}

	private void backConnectionErr(ErrorPacket errPkg, BackendConnection conn) {
		endRunning();
		
		ServerConnection source = session.getSource();
		String errUser = source.getUser();
		String errHost = source.getHost();
		int errPort = source.getLocalPort();
		
		String errmgs = " errno:" + errPkg.errno + " " + new String(errPkg.message);
		LOGGER.warn("execute  sql err :" + errmgs + " con:" + conn 
				+ " frontend host:" + errHost + "/" + errPort + "/" + errUser);
		
		session.releaseConnectionIfSafe(conn, LOGGER.isDebugEnabled(), false);
		
		source.setTxInterrupt(errmgs);
		
		/**
		 * TODO: 修复全版本BUG
		 * 
		 * BUG复现：
		 * 1、MysqlClient:  SELECT 9223372036854775807 + 1;
		 * 2、MyCatServer:  ERROR 1690 (22003): BIGINT value is out of range in '(9223372036854775807 + 1)'
		 * 3、MysqlClient: ERROR 2013 (HY000): Lost connection to MySQL server during query
		 * 
		 * Fixed后
		 * 1、MysqlClient:  SELECT 9223372036854775807 + 1;
		 * 2、MyCatServer:  ERROR 1690 (22003): BIGINT value is out of range in '(9223372036854775807 + 1)'
		 * 3、MysqlClient: ERROR 1690 (22003): BIGINT value is out of range in '(9223372036854775807 + 1)'
		 * 
		 */		
		// 由于 pakcetId != 1 造成的问题 
		errPkg.packetId = 1;		
		errPkg.write(source);
		
		recycleResources();
	}


	/**
	 * insert/update/delete
	 * 
	 * okResponse()：读取data字节数组，组成一个OKPacket，并调用ok.write(source)将结果写入前端连接FrontendConnection的写缓冲队列writeQueue中，
	 * 真正发送给应用是由对应的NIOSocketWR从写队列中读取ByteBuffer并返回的
	 */
	@Override
	public void okResponse(byte[] data, BackendConnection conn) {
		LOGGER.info("===========okResponse========" + BytesHexStrTranslate.bytesToHexFunWithSpace(data));
		this.netOutBytes += data.length;
		
		boolean executeResponse = conn.syncAndExcute();		
		if (executeResponse) {
			ServerConnection source = session.getSource();
			OkPacket ok = new OkPacket();
			ok.read(data);
            boolean isCanClose2Client =(!rrs.isCallStatement()) ||(rrs.isCallStatement() &&!rrs.getProcedure().isResultSimpleValue());
			if (rrs.isLoadData()) {				
				byte lastPackId = source.getLoadDataInfileHandler().getLastPackId();
				ok.packetId = ++lastPackId;// OK_PACKET
				source.getLoadDataInfileHandler().clear();
				
			} else if (isCanClose2Client) {
				ok.packetId = ++packetId;// OK_PACKET
			}


			if (isCanClose2Client) {
				session.releaseConnectionIfSafe(conn, LOGGER.isDebugEnabled(), false);
				endRunning();
			}
			ok.serverStatus = source.isAutocommit() ? 2 : 1;
			recycleResources();

			if (isCanClose2Client) {
				source.setLastInsertId(ok.insertId);
				ok.write(source);
			}
            
			this.affectedRows = ok.affectedRows;
			
			source.setExecuteSql(null);
			// add by lian
			// 解决sql统计中写操作永远为0
			QueryResult queryResult = new QueryResult(session.getSource().getUser(), 
					rrs.getSqlType(), rrs.getStatement(), affectedRows, netInBytes, netOutBytes, startTime, System.currentTimeMillis(),0);
			QueryResultDispatcher.dispatchQuery( queryResult );
		}
	}

	
	/**
	 * select 
	 * 
	 * 行结束标志返回时触发，将EOF标志写入缓冲区，最后调用source.write(buffer)将缓冲区放入前端连接的写缓冲队列中，等待NIOSocketWR将其发送给应用
	 */
	@Override
	public void rowEofResponse(byte[] eof, BackendConnection conn) {

		this.netOutBytes += eof.length;
		
		ServerConnection source = session.getSource();
		conn.recordSql(source.getHost(), source.getSchema(), node.getStatement());
        // 判断是调用存储过程的话不能在这里释放链接
		if (!rrs.isCallStatement()||(rrs.isCallStatement()&&rrs.getProcedure().isResultSimpleValue())) 
		{
			session.releaseConnectionIfSafe(conn, LOGGER.isDebugEnabled(), false);
			endRunning();
		}

		eof[3] = ++packetId;
		buffer = source.writeToBuffer(eof, allocBuffer());
		int resultSize = source.getWriteQueue().size()*MycatServer.getInstance().getConfig().getSystem().getBufferPoolPageSize();
		resultSize=resultSize+buffer.position();
		MiddlerResultHandler middlerResultHandler = session.getMiddlerResultHandler();

		if(middlerResultHandler !=null ){
			middlerResultHandler.secondEexcute(); 
		} else{
			source.write(buffer);
		}
		source.setExecuteSql(null);
		//TODO: add by zhuam
		//查询结果派发
		QueryResult queryResult = new QueryResult(session.getSource().getUser(), 
				rrs.getSqlType(), rrs.getStatement(), affectedRows, netInBytes, netOutBytes, startTime, System.currentTimeMillis(),resultSize);
		QueryResultDispatcher.dispatchQuery( queryResult );

		//清理rowResponse的临时内容
		columnFilterIndexList.clear();
		columnSkipIndexList.clear();
		columnSourceIndexList.clear();
	}

	/**
	 * lazy create ByteBuffer only when needed
	 * 
	 * @return
	 */
	private ByteBuffer allocBuffer() {
		if (buffer == null) {
			buffer = session.getSource().allocate();
		}
		return buffer;
	}

	/**
	 * select
	 * 
	 * 元数据返回时触发，将header和元数据内容依次写入缓冲区中
	 */	
	@Override
	public void fieldEofResponse(byte[] header, List<byte[]> fields,
			byte[] eof, BackendConnection conn) {

		this.header = header;
		//this.fields = fields;
		MiddlerResultHandler middlerResultHandler = session.getMiddlerResultHandler();
        if(null !=middlerResultHandler ){
			return;
		}
		/***************************modify start **********************************/
		this.netOutBytes += header.length;
		header[3] = ++packetId;
		ServerConnection source = session.getSource();
		buffer = source.writeToBuffer(header, allocBuffer());
		/******保持以上header不变******/
		List<byte[]> lstFiledBytes = new ArrayList<byte[]>();
		for (int i = 0, len = fields.size(); i < len; ++i) {
			columnSourceIndexList.add(i);
			byte[] field = fields.get(i);
			// 保存field信息
			FieldPacket fieldPk = new FieldPacket();
			fieldPk.read(field);

			lstFiledBytes.add(field);
		}
		//放到本处再加,如果过滤了加密字段,最终的字段会减少
		this.fields = lstFiledBytes;
		LOGGER.info("=======sourceFiledSize:[" + fields.size() + "],lstFiledBytes.size:[" + lstFiledBytes.size() + "]");
		/***
		 * 遍历过滤后的field.
		 * 如原来:
		 * fields=name|age|content
		 * 过滤掉name后:
		 * lstFiledBytes=age|content
		 */
		for (int i = 0, len = lstFiledBytes.size(); i < len; ++i) {
			byte[] field = lstFiledBytes.get(i);
			field[3] = ++packetId;

			// 保存field信息
			FieldPacketWrapper fieldPkWrapper = new FieldPacketWrapper();
			fieldPkWrapper.read(field);
			/****
			 * 当前查询的表名:
			 */
			String currentTableName = new String(fieldPkWrapper.orgTable);
			/***
			 * 当前表下的加密字段
			 */
			List<String> encryptFieldList = CustomerConfig.getEncryptField(currentTableName);

			//如果是加密字段,进行改写
			/***
			 * name_xx
			 */
			String fieldName = new String(fieldPkWrapper.orgName);
			/***
			 * name
			 * content
			 */
			for (String encryptField : encryptFieldList) {
				/***
				 * name_xx==name+"_xx"?
				 */
				if (fieldName.equals(encryptField + CustomerConfig.encryptedSuffix)) {
					if (!(fieldName.equals(new String(fieldPkWrapper.name)))) {
						//orgName和name不一致，则查询时采用昵称。最终应该展现昵称的字段。
						fieldPkWrapper.orgName = fieldPkWrapper.name;
					} else {
						//重新命名。
						fieldPkWrapper.name = encryptField.getBytes();
						fieldPkWrapper.orgName = fieldPkWrapper.name;

					}
					/***
					 * 标示第几个字段被改过。后续内容也要同步修改
					 */
					columnFilterIndexList.add(i);
					break;
				}
			}

			fieldPackets.add(fieldPkWrapper);

			//重新计算Filed的字节长度
			ByteBuffer newFieldBuffer = ByteBuffer.allocate(4096);//new
			fieldPkWrapper.object2Bytes(newFieldBuffer);
			newFieldBuffer.flip();

			byte[] newFiled = new byte[newFieldBuffer.remaining()];
			ByteBufferUtil.arrayCopy(newFieldBuffer, 0, newFiled, 0, newFiled.length);

			/***
			 * 把修改后的字段，转成byte，按原规则写入。
			 */
			buffer = source.writeToBuffer(newFiled, buffer);

		}
		/***************************modify end **********************************/
		/*****************************source start **********************************/
/*		this.netOutBytes += header.length;
		for (int i = 0, len = fields.size(); i < len; ++i) {
			byte[] field = fields.get(i);
			this.netOutBytes += field.length;
		}

		header[3] = ++packetId;
		ServerConnection source = session.getSource();
		buffer = source.writeToBuffer(header, allocBuffer());
		for (int i = 0, len = fields.size(); i < len; ++i) {
			byte[] field = fields.get(i);
			field[3] = ++packetId;
			
			 // 保存field信息
 			FieldPacket fieldPk = new FieldPacket();
 			fieldPk.read(field);
 			fieldPackets.add(fieldPk);
			
			buffer = source.writeToBuffer(field, buffer);
		}*/
		/*******************************source end********************************/


		fieldCount = fieldPackets.size();
		
		eof[3] = ++packetId;
		buffer = source.writeToBuffer(eof, buffer);

		if (isDefaultNodeShowTable) {
			
			for (String name : shardingTablesSet) {
				RowDataPacket row = new RowDataPacket(1);
				row.add(StringUtil.encode(name.toLowerCase(), source.getCharset()));
				row.packetId = ++packetId;
				buffer = row.write(buffer, source, true);
			}
			
		} else if (isDefaultNodeShowFullTable) {
			
			for (String name : shardingTablesSet) {
				RowDataPacket row = new RowDataPacket(1);
				row.add(StringUtil.encode(name.toLowerCase(), source.getCharset()));
				row.add(StringUtil.encode("BASE TABLE", source.getCharset()));
				row.packetId = ++packetId;
				buffer = row.write(buffer, source, true);
			}
		}
	}

	/**
	 * select 
	 * 
	 * 行数据返回时触发，将行数据写入缓冲区中
	 */
	@Override
	public void rowResponse(byte[] row, BackendConnection conn) {

		//this.netOutBytes += row.length;
		this.selectRows++;

		if (isDefaultNodeShowTable || isDefaultNodeShowFullTable) {
			RowDataPacket rowDataPacket = new RowDataPacket(1);
			rowDataPacket.read(row);
			String table = StringUtil.decode(rowDataPacket.fieldValues.get(0), session.getSource().getCharset());
			if (shardingTablesSet.contains(table.toUpperCase())) {
				return;
			}
		}
		row[3] = ++packetId;

		byte[] newRow = row;//后续要更改
		/*******改写 start***********/
		List<Object> resultList = rowDataChangedNew(row);
		RowDataPacket rowDataPkNew = (RowDataPacket) resultList.get(0);
		newRow = (byte[]) resultList.get(1);
		/*******改写 end***********/
		if (prepared) {

			BinaryRowDataPacket binRowDataPk = new BinaryRowDataPacket();
			binRowDataPk.read(fieldPackets, rowDataPkNew);
			binRowDataPk.packetId = rowDataPkNew.packetId;
//			binRowDataPk.write(session.getSource());
			/*
			 * [fix bug] : 这里不能直接将包写到前端连接,
			 * 因为在fieldEofResponse()方法结束后buffer还没写出,
			 * 所以这里应该将包数据顺序写入buffer(如果buffer满了就写出),然后再将buffer写出
			 */
			buffer = binRowDataPk.write(buffer, session.getSource(), true);
		} else {

			MiddlerResultHandler middlerResultHandler = session.getMiddlerResultHandler();
			if (null == middlerResultHandler) {
				buffer = session.getSource().writeToBuffer(newRow, allocBuffer());
			}else{
				if (middlerResultHandler instanceof MiddlerQueryResultHandler) {
					byte[] rv = ResultSetUtil.getColumnVal(newRow, fields, 0);//如果有过滤，则fields为剔除过滤后的列数
					String rowValue = rv == null ? "" : new String(rv);
					middlerResultHandler.add(rowValue);
				}
			}

		}
		this.netOutBytes += newRow.length;
	}

	/***
	 * 行数据改写,改写完成后，返回封装的Packet及字节数。
	 */
	private List<Object> rowDataChangedNew(byte[] rowSourceByte) {
		{
			RowDataPacket rowDataPk = new RowDataPacket(fieldCount);//fieldCount为本次查询制定的列.数值查询时固定.不受列剔除的影响.
			rowDataPk.read(rowSourceByte);

			/***
			 * 如果fieldList有过滤，则在下面进行处理剔除列对应内容的同步剔除.
			 */
			List<byte[]> fieldValueList = rowDataPk.fieldValues;
			/***
			 * 1.过滤掉不需要列对应的内容
			 */
			for (Integer indexField : columnSkipIndexList) {
				byte[] removeValue = fieldValueList.get(indexField);
				//移除
				fieldValueList.remove(indexField);
				LOGGER.info("====removedValue:[" + BytesHexStrTranslate.bytesToChar(removeValue) + "]");
			}
			/***
			 * 2.对需要更改列的内容进行替换。
			 */
			for (Integer indexField : columnFilterIndexList) {
				/***
				 * 取出被修改过field列，对应的内容
				 */
				byte[] currentFieldValue = fieldValueList.get(indexField);
				if (currentFieldValue == null) {
					continue;
				}
				/***
				 * 内容替换
				 */
				byte[] newValue = EncrptUtil.doDecipher(new String(currentFieldValue)).getBytes();
				fieldValueList.set(indexField, newValue);
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("======将原来的内容[" + new String(currentFieldValue) + "] 替换为:[" + new String(newValue) + "]");
				}
			}
			/***
			 * 内容替换后,需要重新计算包的长度等
			 */
			//重新计算Filed的字节长度
			ByteBuffer newFieldBuffer = ByteBuffer.allocate(4096);//new
			rowDataPk.object2Bytes(newFieldBuffer);
			newFieldBuffer.flip();
			/**重新计算后的字节**/
			byte[] newRowByte = new byte[newFieldBuffer.remaining()];
			ByteBufferUtil.arrayCopy(newFieldBuffer, 0, newRowByte, 0, newRowByte.length);

			//创建一个新的行对象封装类
			RowDataPacket rowDataPkNew = new RowDataPacket(fieldCount - columnSkipIndexList.size());
			rowDataPkNew.read(newRowByte);
			/***
			 * 需要返回2给对象，因此采用当前的做法返回
			 * TODO
			 */
			List<Object> resultList = new ArrayList<Object>();
			//改写后的数据包封装类
			resultList.add(rowDataPkNew);
			//数据包封装类对于的字节
			resultList.add(newRowByte);
			return resultList;

		}
	}

	@Override
	public void writeQueueAvailable() {

	}

	@Override
	public void connectionClose(BackendConnection conn, String reason) {
		ErrorPacket err = new ErrorPacket();
		err.packetId = ++packetId;
		err.errno = ErrorCode.ER_ERROR_ON_CLOSE;
		err.message = StringUtil.encode(reason, session.getSource()
				.getCharset());
		this.backConnectionErr(err, conn);

	}

	public void clearResources() {

	}

	@Override
	public void requestDataResponse(byte[] data, BackendConnection conn) {
		LoadDataUtil.requestFileDataResponse(data, conn);
	}
	
	public boolean isPrepared() {
		return prepared;
	}

	public void setPrepared(boolean prepared) {
		this.prepared = prepared;
	}

	@Override
	public String toString() {
		return "SingleNodeHandler [node=" + node + ", packetId=" + packetId + "]";
	}

}