#include "RabbitMQOperation.h"

#define ERROR_MASK (0x00FF) 
#define ERROR_CATEGORY_MASK (0xFF00)

RabbitMQOperation::RabbitMQOperation()
: m_con(NULL)
, m_pSocket(NULL)
, m_isConfigError(false)
{
    m_Timeout.tv_sec = 1;
    m_Timeout.tv_usec = 0;
    memset(&m_status, 0, sizeof(bool) * Status_cnt);
    memset(&m_szStatusReason, 0, sizeof(TCHAR) * 64);
    _tcsncpy_s(m_szStatusReason, _countof(m_szStatusReason), UNKNOWN_REASON, _TRUNCATE);
    m_nConsumerCount = 0;
    m_nPendingMsg = 0;

	memset(&queuename, 0, sizeof(queuename));
}

RabbitMQOperation::~RabbitMQOperation()
{
    if(NULL != m_con)
    {
        // after line will cause failure  
        amqp_channel_close(m_con, 1, AMQP_REPLY_SUCCESS);// "Closing channel"
        amqp_connection_close(m_con, AMQP_REPLY_SUCCESS);// "Closing connection"
        amqp_destroy_connection(m_con);// "Ending connection"
        m_con = NULL;
    }
}

bool RabbitMQOperation::ConnectMQ( LPCTSTR host, int port, LPCTSTR loginUser, LPCTSTR loginPwd )
{
    DisConnectMQ();

    if (NULL == m_con)
    {
        m_con = amqp_new_connection();
    }
    m_pSocket = amqp_tcp_socket_new(m_con);

    if ( NULL == m_con || NULL == m_pSocket)
    {
        ParseError(_T("RabbitMQOperation::ConnectMQ(): Failed to create TCP socket because of out of memory."));  //amqp_status_enum
        return false;
    }

    int status;
    RpcReply reply;
    //status = amqp_socket_open_noblock(m_pSocket, host, port, &m_Timeout);
	status = amqp_socket_open(m_pSocket, host, port);

    if(status)
    {
        ParseError((amqp_status_enum)status, _T("ConnectMQ(): Failed to open TCP socket because of host or port."));
        return false;
    }
    reply = amqp_login(m_con, "/", 0, 131072, 0, AMQP_SASL_METHOD_PLAIN, loginUser, loginPwd);
    // check reply response
    if(reply.reply_type != AMQP_RESPONSE_NORMAL)
    {
        ParseError(reply, CONNECTION, _T("Login in RabbitMQ"));
        return false;
    }

    SetStatus( CONNECTION, true);


    amqp_channel_open(m_con, 1);
    reply = amqp_get_rpc_reply(m_con);// "Opening channel"

    if(reply.reply_type != AMQP_RESPONSE_NORMAL)
    {
        ParseError(reply, CHANNEL, _T("Opening channel"));
        return false;
    }
    SetStatus( CHANNEL, true);

    // Log the connection address and port to file
    {
        int sktfd = amqp_get_sockfd(m_con);
        struct sockaddr_in localAddr;
        int x = sizeof(localAddr);

        struct sockaddr_in peerAddr;
        int y = sizeof(peerAddr);

        USHORT usLocalPort = 0u;
        USHORT usPeerPort = 0u;
        CString csLocalIp(_T(""));
        CString csPeerIp(_T(""));

        if (0 == getsockname(sktfd, (struct sockaddr*)&localAddr, &x))
        {
            usLocalPort = ntohs(localAddr.sin_port);
            csLocalIp = inet_ntoa(localAddr.sin_addr);
        }

        if (0 == getpeername(sktfd, (struct sockaddr*)&peerAddr, &y))
        {
            usPeerPort = ntohs(peerAddr.sin_port);
            csPeerIp = inet_ntoa(peerAddr.sin_addr);
        }
    }

    return true;
}

void RabbitMQOperation::DisConnectMQ()
{
    if(NULL != m_con)
    {
        // after line will cause failure  
        amqp_channel_close(m_con, 1, AMQP_REPLY_SUCCESS);// "Closing channel"
        amqp_connection_close(m_con, AMQP_REPLY_SUCCESS);// "Closing connection"
        amqp_destroy_connection(m_con);// "Ending connection"
        m_con = NULL;
    }

    m_isConfigError = false;
}

bool RabbitMQOperation::DeclareExchage( LPCTSTR exchangeName, LPCTSTR exchangeType )
{
    if (NULL == m_con)
    {
        return false;
    }

    RpcReply reply;

    amqp_exchange_declare(m_con, 1, amqp_cstring_bytes(exchangeName),
        amqp_cstring_bytes(exchangeType),
        0, 1, amqp_empty_table);
    reply = amqp_get_rpc_reply(m_con);


    if(reply.reply_type != AMQP_RESPONSE_NORMAL)
    {
        ParseError(reply, EXCHANGE, _T("Declaring exchange"));
        return false;
    }

    SetStatus( EXCHANGE, true);
    return true;
}

bool RabbitMQOperation::BindingQueue( LPCTSTR queueName, LPCTSTR exchangeName, LPCTSTR bindingKey )
{
    if (NULL == m_con)
    {
        return false;
    }
    RpcReply reply;

    amqp_queue_declare_ok_t * declareok = amqp_queue_declare(m_con, 1, amqp_cstring_bytes(queueName), 0, 1, 0, 0,amqp_empty_table);
    reply = amqp_get_rpc_reply(m_con);

    if(reply.reply_type != AMQP_RESPONSE_NORMAL)
    {
        ParseError(reply, QUEUE, _T("Declaring queue"));
        return false;
    }

	queuename = amqp_bytes_malloc_dup(declareok->queue);
  if (queuename.bytes == NULL) 
	{
		return 1;
  }

    //amqp_queue_bind(m_con, 1,amqp_cstring_bytes(queueName),amqp_cstring_bytes(exchangeName),amqp_cstring_bytes(bindingKey),amqp_empty_table);
	amqp_queue_bind(m_con, 1,queuename,amqp_cstring_bytes(exchangeName),amqp_cstring_bytes(bindingKey),amqp_empty_table);

    reply = amqp_get_rpc_reply(m_con);
    if(reply.reply_type != AMQP_RESPONSE_NORMAL)
    {
        ParseError(reply, QUEUE, _T("Binding queue"));
        return false;
    }

    SetStatus( QUEUE, true);
    m_nConsumerCount = declareok->consumer_count;
    m_nPendingMsg  = declareok->message_count;
    if(0 == m_nConsumerCount)
    {
        m_status[CONSUMER] = false;
    }
    return true;
}

bool RabbitMQOperation::DeclareConsumer( LPCTSTR lpszQueueName )
{
    if (NULL == m_con)
    {
        return false;
    }
    amqp_rpc_reply_t reply;
    //amqp_basic_consume(m_con, 1, amqp_cstring_bytes(lpszQueueName), amqp_empty_bytes, 0, 1, 0, amqp_empty_table);
	amqp_basic_consume(m_con, 1, queuename, amqp_empty_bytes, 0, 1, 0, amqp_empty_table);

    reply = amqp_get_rpc_reply(m_con);
    if(reply.reply_type != AMQP_RESPONSE_NORMAL)
    {
        ParseError(reply, CONSUMER, _T("Declare consumer")); 
        return false;
    }

    SetStatus( CONSUMER, true);
    return true;
}

bool RabbitMQOperation::PublishMsg( LPCTSTR exchangeName, LPCTSTR routingKey, MQ_MESSAGE* pMsg, bool bConfirmSelect )
{
    if (NULL == m_con)
    {
        return false;
    }
    int status;
    amqp_rpc_reply_t reply;
    amqp_bytes_t body;
    body.len = pMsg->dwMQMsgLen;
	body.bytes = (void *)pMsg->pMQMsgBuf;

	// RPC 
	if (pMsg->enableRPC) 
	{
		/* 
		set properties 
		*/
		//amqp_bytes_t replyTo = amqp_bytes_malloc(pMsg->dwReplyToLen);
		amqp_bytes_t corre = amqp_bytes_malloc(pMsg->dwCorrIdLen);

		//::memcpy(replyTo.bytes, pMsg->pReplyTo, pMsg->dwReplyToLen);
		//replyTo.len = pMsg->dwReplyToLen;
		::memcpy(corre.bytes, pMsg->pCorrId, pMsg->dwCorrIdLen);
		corre.len = pMsg->dwCorrIdLen;
		
		amqp_basic_properties_t props;
		props._flags = AMQP_BASIC_DELIVERY_MODE_FLAG |
						//AMQP_BASIC_REPLY_TO_FLAG |
						AMQP_BASIC_CORRELATION_ID_FLAG;
		//props.reply_to = amqp_bytes_malloc_dup(replyTo);
		props.correlation_id = amqp_bytes_malloc_dup(corre);
		props.delivery_mode = 2;

		std::string queueName;
		queueName.assign((const char*)pMsg->pReplyTo, pMsg->dwReplyToLen);

		status = amqp_basic_publish(m_con,
									1,
									amqp_cstring_bytes(""),
									amqp_cstring_bytes(queueName.c_str()),
									0,
									0,
									&props,
									body);
	}
	else
	{
		/* 
		set properties 
		*/ 
		amqp_basic_properties_t props;
		props._flags = AMQP_BASIC_DELIVERY_MODE_FLAG;
		props.delivery_mode = 2; /* persistent delivery mode */

		status = amqp_basic_publish(m_con,1,
			amqp_cstring_bytes(exchangeName),
			amqp_cstring_bytes(routingKey),
			0,0, &props, body);
	}

	if(status < 0)
	{
		ParseError((amqp_status_enum)status, _T("PublishMsg() Failed to send a message to MQ.")); 
		reply = amqp_get_rpc_reply(m_con);
		if(reply.reply_type != AMQP_RESPONSE_NORMAL)
		{
			ParseError(reply, PRODUCER, _T("PublishMsg"));
		}
		return false;
	}

    amqp_maybe_release_buffers(m_con); // Avoid possible memory leak

    return true;
}

bool RabbitMQOperation::ConsumeMsg( MQ_MESSAGE& rMQMsg, DWORD dwMilliseconds, bool& bTimeOut )
{
    if(NULL == m_con)
        return false;

	amqp_rpc_reply_t reply;
	memset(&reply,0,sizeof(amqp_rpc_reply_t));

	amqp_envelope_t envelope;
	memset(&envelope,0,sizeof(amqp_envelope_t));
	

	amqp_maybe_release_buffers(m_con); // Avoid possible memory leak
	timeval m_time;
	m_time.tv_sec = dwMilliseconds/1000;
	m_time.tv_usec = (dwMilliseconds%1000)*1000;
	
	reply = amqp_consume_message(m_con, &envelope, &m_time, 0);//time out 1 second

	if (AMQP_RESPONSE_NORMAL != reply.reply_type)
	{
		bTimeOut = ( (reply.reply_type == AMQP_RESPONSE_LIBRARY_EXCEPTION) && (reply.library_error == AMQP_STATUS_TIMEOUT) );
		if (!bTimeOut)
		{
			ParseError(reply, CONSUMER, _T("Get message"));
		}
		return false;
	}

	bool bRet = false;
	amqp_bytes_t& rTheBody = envelope.message.body;
	amqp_basic_properties_t& rThePro = envelope.message.properties;

	if (rTheBody.len > 0)
	{
		rMQMsg.pMQMsgBuf = new byte[rTheBody.len];
		rMQMsg.pReplyTo = new byte[rThePro.reply_to.len];
		rMQMsg.pCorrId = new byte[rThePro.correlation_id.len];
		if (NULL != rMQMsg.pMQMsgBuf && 
			NULL != rMQMsg.pReplyTo &&
			NULL != rMQMsg.pCorrId)
		{
			::memcpy(rMQMsg.pReplyTo, rThePro.reply_to.bytes, rThePro.reply_to.len);
			rMQMsg.dwReplyToLen = rThePro.reply_to.len;

			::memcpy(rMQMsg.pCorrId, rThePro.correlation_id.bytes, rThePro.correlation_id.len);
			rMQMsg.dwCorrIdLen = rThePro.correlation_id.len;

			::memcpy(rMQMsg.pMQMsgBuf, rTheBody.bytes, rTheBody.len);
			rMQMsg.dwMQMsgLen = (DWORD)rTheBody.len;
			bRet = true;

		}
		else
		{
			LoggerFatal(_T("RabbitMQOperation::ConsumeMsg() NULL AGAMMQ_MESSAGE."));
		}
	}
	else
	{
		LoggerError(_T("RabbitMQOperation::ConsumeMsg() Message Body is null."));
	}

	amqp_destroy_envelope(&envelope);

	return bRet;
}

void RabbitMQOperation::ParseError(char const *context)
{
    LoggerTrace1(_T("%s\n"), context);
    return;
}

void RabbitMQOperation::ParseError( amqp_status_enum status, char const *context )
{
    LoggerTrace1(_T("%s %s\n"), context, amqp_error_string2(status));
    return;
}


void RabbitMQOperation::ParseError( amqp_rpc_reply_t reply, RabbitMQStatus status, char const *context )
{
    switch (reply.reply_type) 
    {
    case AMQP_RESPONSE_NORMAL:
        return;

    case AMQP_RESPONSE_NONE:
        LoggerTrace1(_T("%s missing RPC reply type!\n"), context);
        break;

    case AMQP_RESPONSE_LIBRARY_EXCEPTION:
        {
            LoggerTrace1(_T( "%s %s\n"), context, amqp_error_string2(reply.library_error));
            int code = reply.library_error;
            size_t category = (((-code) & ERROR_CATEGORY_MASK) >> 8); // EC_base = 0
            size_t error = (-code) & ERROR_MASK;
            if(0 == category && AMQP_STATUS_SOCKET_ERROR == error)
            {
                SetStatus(CONNECTION,false);
                _tcsncpy_s(m_szStatusReason, _countof(m_szStatusReason), CONNECTION_BROKEN, _TRUNCATE);
            }
            else
            {
                SetStatus(CONNECTION,false);
                _tcsncpy_s(m_szStatusReason, _countof(m_szStatusReason), UNKNOWN_REASON, _TRUNCATE);
            }
        }
        break;

    case AMQP_RESPONSE_SERVER_EXCEPTION:
        switch (reply.reply.id) 
        {
        case AMQP_CONNECTION_CLOSE_METHOD: 
            {
                amqp_connection_close_t *m = (amqp_connection_close_t *) reply.reply.decoded;
                LoggerTrace1(_T( "%s server connection error %d, message: %.*s\n"),
                    context,
                    m->reply_code,
                    (int) m->reply_text.len, (char *) m->reply_text.bytes);
                if(AMQP_CONNECTION_FORCED == m->reply_code)
                {
                    SetStatus(CONNECTION,false);
                    _tcsncpy_s(m_szStatusReason, _countof(m_szStatusReason), CONNECTION_BROKEN, _TRUNCATE);
                }
                break;
            }
        case AMQP_CHANNEL_CLOSE_METHOD: 
            {
                amqp_channel_close_t *m = (amqp_channel_close_t *) reply.reply.decoded;
                LoggerTrace1(_T( "%s: server channel error %d, message: %.*s\n"),
                    context,
                    m->reply_code,
                    (int) m->reply_text.len, (char *) m->reply_text.bytes);
                if (AMQP_NOT_FOUND == m->reply_code)// no exchange or no queue
                {
                    if (QUEUE == status)
                    {
                        SetStatus(EXCHANGE,false);
                        _tcsncpy_s(m_szStatusReason, _countof(m_szStatusReason), NO_EXCHANGE, _TRUNCATE);
                    }
                    else if(CONSUMER == status)
                    {
                        SetStatus(QUEUE,false);
                        _tcsncpy_s(m_szStatusReason, _countof(m_szStatusReason), NO_QUEUE, _TRUNCATE);
                    }
                }
                else if(AMQP_PRECONDITION_FAILED == m->reply_code)
                {
                    if (EXCHANGE == status)
                    {
                        SetStatus(EXCHANGE,false);
                        m_isConfigError = true;
                        _tcsncpy_s(m_szStatusReason, _countof(m_szStatusReason),EXCHANGE_CONFIG_MISMATCH, _TRUNCATE);
                    }
                    else if(QUEUE == status)
                    {
                        SetStatus(QUEUE,false);
                        m_isConfigError = true;
                        _tcsncpy_s(m_szStatusReason, _countof(m_szStatusReason), QUEUE_CONFIG_MISMATCH, _TRUNCATE);
                    }
                }
                break;
            }
        default:
            LoggerTrace1(_T( "%s unknown server error, method id 0x%08X\n"), context, reply.reply.id);
            break;
        }
        break;
    }
    return;
}

void RabbitMQOperation::SetStatus( RabbitMQStatus part, bool status )
{
    if(false == status)
    {
        for (int i = part;i < Status_cnt; ++i)
        {
            m_status[i] = false;
        }
    }
    else
    {
        for (int i = part;i >= 0; --i)
        {
            m_status[i] = true;
        }
    }
}

bool RabbitMQOperation::isConnected()
{
    return m_status[CONNECTION];
}

bool RabbitMQOperation::hasConsumer()
{
    return m_status[CONSUMER];
}

bool RabbitMQOperation::IsConfigError()
{
    return m_isConfigError;
}

void RabbitMQOperation::TurnOnConfirmSelect( bool bConfirmSelect )
{
    if (bConfirmSelect)
    {
        LoggerInfo(_T("RabbitMQOperation::TurnOnConfirmSelect() Turn On Confirm Select"));
        amqp_confirm_select(m_con, 1); // open select confirm on channel
    }
}

bool RabbitMQOperation::ConfirmMessage()
{
    amqp_frame_t frame;
    if (AMQP_STATUS_OK != amqp_simple_wait_frame(m_con, &frame)) {
        return false;
    }
    if (AMQP_FRAME_METHOD == frame.frame_type) {
        amqp_method_t method = frame.payload.method;
        switch(method.id){
            case AMQP_BASIC_ACK_METHOD:
              /* if we've turned publisher confirms on, and we've published a message
               * here is a message being confirmed
               */
                return true;
              break;
            default:
                ParseError(_T("In Confirm mode, MQ doesn't receive the message. Retry to send message."));
                return false;
        }
    }
    ParseError(_T("In Confirm mode, MQ doesn't receive the message. Retry to send message."));
    return false;
}
