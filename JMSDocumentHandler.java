package org.telligen.cpci.jms;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;
import java.util.Arrays;
import org.dom4j.Document;
import org.springframework.jms.connection.SingleConnectionFactory;
import org.springframework.jms.core.JmsOperations;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessagePostProcessor;
import org.telligen.cpci.utils.CPCIConstants;
import org.telligen.cpciconsumer.shared.delegate.LoggerDelegate;
import org.telligen.cpciconsumer.shared.domain.FileContent;
import java.sql.Timestamp;
import java.util.Date;
/**
 * The Class JMSDocumentHandler.
 * Responsible for managing the JmsOperations used to send reply messages. 
 */
public class JMSDocumentHandler extends LoggerDelegate{
	//private final Logger logger = LoggerFactory.getLogger(getClass());
	//protected final Log wslogger = LogFactory.getLog(getClass());

	/** The expiry time. */
	private int expiryTime = CPCIConstants.GLOBAL_DEFAULT_EXPIRY;

	/** The message jms template practice request aws. */
	private JmsOperations messageJmsTemplatePracticeRequestAWS;
	
	/** The message jms template practice reply aws. */
	private JmsOperations messageJmsTemplatePracticeReplyAWS;
		
	//user Queues
	/** The message jms template user request aws. */
	private JmsOperations messageJmsTemplateUserRequestAWS;
		
	/** The message jms template user reply aws. */
	private JmsOperations messageJmsTemplateUserReplyAWS;
	
	/**
	 * Consume document.
	 * Attach a MessagePostProcessor if the message document adheres to root node convention <message>
	 *
	 * @param document the document
	 * @param messageId the message id
	 */
		 
	
	public void consumeDocument(Document document, String messageId) {
		CorrelationIdPostProcessor mpp = null;
		if(messageId != null){
			 info("WMQ-0006-CorrelationIdPostProcessor"); 
			 info("WMQ-0006-CorrelationIdPostProcessor  messageId  is "  +messageId); 
			 mpp = new CorrelationIdPostProcessor(messageId);
			 info("WMQ-0007-CorrelationIdPostProcessor MPP  IS : " + mpp);
		}
		if ("message".equals(document.getRootElement().getName())) {
			info("WMQ-0008-CorrelationIdPostProcessor MPP  IS : " + mpp);
			consumeMessage(document, mpp);
		}
		else {
			throw new IllegalArgumentException("Document type not supported: "
					+ document.asXML());
		}
	}


	/**
	 * Consume file content message.
	 * Create a MessagePostProcessor for File Messages and process the File payload.
	 *
	 * @param fileChunk the file chunk
	 * @param message the message
	 * @param messageId the message id
	 * @param fileId the file id
	 * @param userId the user id
	 * @param chunkIndex the chunk index
	 */
	public void consumeFileContentMessage(byte[] fileChunk,  Object message, String messageId, String fileId, String userId, String chunkIndex) {
		CorrelationIdForFilePostProcessor mpp = null;
		if(messageId != null){
			FileContent fc = (FileContent)message;
			 mpp = new CorrelationIdForFilePostProcessor(messageId, fileId, fc.getName(), Integer.toString(fc.getSize()), fc.getChecksum(),fc.getFileType(),userId, chunkIndex);
			 consumeMessage(fileChunk,mpp);
		}
		
	}
	
	

	
	/**
	 * Consume message.
	 * Use message type to determine what JmsOperations field to use to send the message.  
	 *
	 * @param message the message
	 * @param mpp the mpp
	 */
	private void consumeMessage(Object message, MessagePostProcessor mpp) {
		//debug
		info("Putting message on queue");
		info("wsPutting message on queue");
		info("WMQ-0009-CorrelationIdPostProcessor MPP  IS : " + mpp); 
		JmsOperations messageJmsTemplateRequestAWS =null;
		JmsOperations messageJmsTemplateReplyAWS =null;
		if(message instanceof Document){
			info("WMQ-9900");
			Document messageDoc = (Document)message;
			debug(messageDoc.asXML());
			info("WMQ-9901");
		}
		if(mpp == null){
			
			info("WMQ-0010-consumeMessage message is  : " + message); 
			messageJmsTemplatePracticeRequestAWS.convertAndSend(message);
		} else {
			if(mpp instanceof CorrelationIdPostProcessor){
				info("WMQ-9902");
				messageJmsTemplateRequestAWS = messageJmsTemplatePracticeRequestAWS;
				messageJmsTemplateReplyAWS = messageJmsTemplateRequestAWS;
			}
			else if(mpp instanceof CorrelationIdForFilePostProcessor){
				info("WMQ-9903");
				messageJmsTemplateRequestAWS = messageJmsTemplateUserRequestAWS;
				messageJmsTemplateReplyAWS = messageJmsTemplateRequestAWS;
			}
			//debug
			long expiry = System.currentTimeMillis()+ expiryTime;
			info("ws mmp added "+expiry);
						
			try{
				
				info("WMQ-0011-(JmsTemplate)messageJmsTemplateRequestAWS) message is  : " + message); 
				info("WMQ-0012-(JmsTemplate)messageJmsTemplateRequestAWS) mpp is  : " + mpp); 
				 
				      
				    ((JmsTemplate)messageJmsTemplateRequestAWS).convertAndSend(message, mpp);
					
					
					 info("WMQ-0013-After ((JmsTemplate)messageJmsTemplateRequestAWS).convertAndSend(message, mpp)"); 
				 	
					
			}catch(Exception e){
				
				//debug
				info("caught now going to reset connection");
				info("WMQ-99O7 Caught Exception ====>: " + e.getMessage());
				info("WMQ-99O8 Caught Exception ====>: " + e.getLocalizedMessage());
                info("WMQ-99O9 Caught Exception ====>: " + e.getCause());
                info("WMQ-9910 Caught Exception ====>: " + Arrays.toString(e.getStackTrace()));
				
				ConnectionFactory cf1 = ((JmsTemplate)messageJmsTemplateRequestAWS).getConnectionFactory();
				if (cf1 instanceof SingleConnectionFactory) {
					SingleConnectionFactory sf1 = (SingleConnectionFactory)cf1;
					sf1.resetConnection();
				}
				
				ConnectionFactory cf2 = ((JmsTemplate)messageJmsTemplateReplyAWS).getConnectionFactory();
				if (cf2 instanceof SingleConnectionFactory) {
					SingleConnectionFactory sf2 = (SingleConnectionFactory)cf2;
					sf2.resetConnection();
				}
				
				//debug
				debug("and now resending");
				((JmsTemplate)messageJmsTemplateRequestAWS).convertAndSend(message, mpp);
			}
		}
	}

	/**
	 * Gets the message.
	 * Poll the reply queue (given by the reply JmsOperation) for a response message 
	 * matching on messageId.
	 *
	 * @param messageId the message id
	 * @return the message
	 */
	public Message getMessage(String messageId) {
		
		java.util.Date date= new java.util.Date();
	     
		info("WMQ-7777====>: " + (new Timestamp(date.getTime())));
		String messageSelector = generateMessageSelector(messageId);
		info("WMQ-7778====>: " + (new Timestamp(date.getTime())));
		debug("message selector generated: "+messageSelector);
		info("WMQ-7779 - Timestamp Just Before Reading Messages From ReplyToQueue ===>: " + (new Timestamp(date.getTime())));
		Message message = messageJmsTemplatePracticeReplyAWS.receiveSelected(messageSelector);
		info("WMQ-7780 - Timestamp Right After Reading Messages From ReplyToQueue ===>: " + (new Timestamp(date.getTime())));
		//ssage message = messageJmsTemplatePracticeReplyAWS.receive();
		logMessageProperties(message);
		info("WMQ-7781 Message is ====>: " + message);
		return message;
	}
	
	/**
	 * Gets the message for file upload.
	 *Poll the File message reply queue (given by the reply JmsOperation) for a response message 
	 * matching on messageId.
	 * 
	 * @param messageId the message id
	 * @return the message for file upload
	 */
	public Message getMessageForFileUpload(String messageId) {
		String messageSelector = generateMessageSelector(messageId);
		debug("message selector generated: "+messageSelector);
		Message message = messageJmsTemplateUserReplyAWS.receiveSelected(messageSelector);
		logMessageProperties(message);
		return message;
	}
	
	/**
	 *
	 * @param messageId the message id
	 * @return a JMSCorrelationID based selector
	 */
	public String generateMessageSelector(String messageId){
		String messageSelector = //"XPATH '//message[@id=\""+messageId+"\"]'"; 
		"JMSCorrelationID = '" + messageId + "'"; 
		return messageSelector;
	}
	
	/**
	 * The Class CorrelationIdPostProcessor.
	 * Used to set message properties correlationId and replyTo queue.
	 */
	private class CorrelationIdPostProcessor implements MessagePostProcessor {
		
		/** The correlation id. */
		private final String correlationId;
		
		/**
		 * Instantiates a new correlation id post processor.
		 *
		 * @param _correlationId the _correlation id
		 */
		public CorrelationIdPostProcessor(final String _correlationId) {
			this.correlationId = _correlationId;
		}
		
		/* (non-Javadoc)
		 * @see org.springframework.jms.core.MessagePostProcessor#postProcessMessage(javax.jms.Message)
		 */
		@Override
		public Message postProcessMessage(final Message msg) throws JMSException {
			info("WMQ-0015-postProcessMessage msg is : "+msg);
			msg.setJMSCorrelationID(correlationId);
			JmsTemplate replyTemplate = ((JmsTemplate)messageJmsTemplatePracticeReplyAWS);
			Connection conn = replyTemplate.getConnectionFactory().createConnection();
			Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
			Queue replyTo = sess.createQueue(replyTemplate.getDefaultDestinationName());
			info("CorrelationIdPostProcessor replyTo: "+replyTo);
			if(replyTo != null){
				msg.setJMSReplyTo(replyTo);
			}else{
				debug("***********CorrelationIdPostProcessor replyTo: queue is null "+msg);
			}
			conn.close();
			info("WMQ-0015-CorrelationIdPostProcessor replyTo: "+replyTo);
			return msg;
		}
	}
	
	
	
	/**
	 * The Class CorrelationIdForFilePostProcessor.
	 * Used to set message properties correlationId and replyTo queue.
	 * In addition to file message specific properties.
	 */
	private class CorrelationIdForFilePostProcessor implements MessagePostProcessor {
		
		/** The correlation id. */
		private final String correlationId;
		
		/** The file name. */
		private final String fileName;	
		
		/** The file size. */
		private final String fileSize;
		
		/** The checksum. */
		private final String checksum;
		
		/** The file type. */
		private final String fileType;
		
		/** The file id. */
		private final String fileId;
		
		/** The chunk index. */
		private final String chunkIndex;
		
		/** The user id. */
		private final String userId;
		
		/**
		 * Instantiates a new correlation id for file post processor.
		 *
		 * @param _correlationId the _correlation id
		 * @param _fileId the _file id
		 * @param _fileName the _file name
		 * @param _fileSize the _file size
		 * @param _checksum the _checksum
		 * @param _fileType the _file type
		 * @param _userId the _user id
		 * @param _chunkIndex the _chunk index
		 */
		public CorrelationIdForFilePostProcessor(final String _correlationId,final String _fileId, final String _fileName, final String _fileSize, final String _checksum, final String _fileType,final String _userId, final String  _chunkIndex) {
			this.correlationId = _correlationId;
			this.fileName = _fileName;	
			this.fileSize = _fileSize;
			this.checksum = _checksum;
			this.fileId = _fileId;
			this.chunkIndex = _chunkIndex;
			this.fileType = _fileType;
			this.userId = _userId;
			
		}
		
		/* (non-Javadoc)
		 * @see org.springframework.jms.core.MessagePostProcessor#postProcessMessage(javax.jms.Message)
		 */
		@Override
		public Message postProcessMessage(final Message msg) throws JMSException {
			msg.setJMSCorrelationID(correlationId);
			msg.setStringProperty("fileName", fileName);
			msg.setStringProperty("fileSize", fileSize);
			msg.setStringProperty("checksum", checksum);
			msg.setStringProperty("fileType", fileType);
			msg.setStringProperty("fileId", fileId);
			msg.setStringProperty("chunkIndex", chunkIndex);
			msg.setStringProperty("userId", userId);
			JmsTemplate replyTemplate = ((JmsTemplate)messageJmsTemplateUserReplyAWS);
			Connection conn = replyTemplate.getConnectionFactory().createConnection();
			Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
			Queue replyTo = sess.createQueue(replyTemplate.getDefaultDestinationName());
			info("CorrelationIdForFilePostProcessor replyTo: "+replyTo);
			if(replyTo != null){
				msg.setJMSReplyTo(replyTo);
			}
			conn.close();
			return msg;
		}
	}
	
	
	
	
	/**
	 * Sets the message jms template practice request aws.
	 *
	 * @param _messageJmsTemplatePracticeRequestAWS the new message jms template practice request aws
	 */
	public void setMessageJmsTemplatePracticeRequestAWS(
			JmsOperations _messageJmsTemplatePracticeRequestAWS) {
		this.messageJmsTemplatePracticeRequestAWS = _messageJmsTemplatePracticeRequestAWS;
	}

	/**
	 * Sets the message jms template practice reply aws.
	 *
	 * @param _messageJmsTemplatePracticeReplyAWS the new message jms template practice reply aws
	 */
	public void setMessageJmsTemplatePracticeReplyAWS(
			JmsOperations _messageJmsTemplatePracticeReplyAWS) {
		this.messageJmsTemplatePracticeReplyAWS = _messageJmsTemplatePracticeReplyAWS;
	}
	
	/**
	 * Sets the message jms template user request aws.
	 *
	 * @param _messageJmsTemplateUserRequestAWS the new message jms template user request aws
	 */
	public void setMessageJmsTemplateUserRequestAWS(
			JmsOperations _messageJmsTemplateUserRequestAWS) {
		this.messageJmsTemplateUserRequestAWS = _messageJmsTemplateUserRequestAWS;
	}
	
	/**
	 * Sets the message jms template user reply aws.
	 *
	 * @param _messageJmsTemplateUserReplyAWS the new message jms template user reply aws
	 */
	public void setMessageJmsTemplateUserReplyAWS(
			JmsOperations _messageJmsTemplateUserReplyAWS) {
		this.messageJmsTemplateUserReplyAWS = _messageJmsTemplateUserReplyAWS;
	}

	/**
	 * Log message properties.
	 *
	 * @param message the message
	 */
	public void logMessageProperties(Message message){
		try {
			info("delivery mode: "+message.getJMSDeliveryMode());
			info("expiration: "+message.getJMSExpiration());
			info("delivery mode: "+message.getJMSDeliveryMode());
			info("expiration: "+message.getJMSExpiration());
			if(message instanceof TextMessage){
				info("message Type: TextMessage");
			}
			if(message instanceof MapMessage ){
				info("message Type: MapMessage");
			}
			if(message instanceof BytesMessage ){
				info("message Type: BytesMessage");
			}
			if(message instanceof StreamMessage  ){
				info("message Type: Stream of Primitives");
			}
		} catch (JMSException e) {
			error(e.getMessage());
		}
	}
	
	/**
	 * Sets the expiry time.
	 *
	 * @param _expiryTime the new expiry time
	 */
	public void setExpiryTime(int _expiryTime) {
		this.expiryTime = _expiryTime;
	}
}
