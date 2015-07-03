void ConsumerThread::Run()
{
    enum
    {
        Term_Evt = 0,
        MQTimer_Evt,
        RpTimer_Evt,
        Queue_Evt,
 
        Evt_cnt,
    };
 
    HANDLE hEvtsToWait[Evt_cnt] = {
        m_hShutdownEvent,
        m_hConnectionCheckTimer,
        m_hReportQueueStatTimer,
        m_oMyQ.operator HANDLE(),
    };
 
    BOOL bExit = FALSE; 
 
	MQInitialize(); 
 
    while(!bExit)
    {
        DWORD dwRet = ::WaitForMultipleObjects(Evt_cnt, hEvtsToWait, FALSE, 1); // time out to check the message
        switch(dwRet)
        {
        case WAIT_OBJECT_0 + Term_Evt:
            // do Clean up before exit loop
            TermProc();
            bExit = TRUE; //Exit the loop
            break;
 
        case WAIT_OBJECT_0 + MQTimer_Evt:
	    MQTimerProc();
            break;
 
        case WAIT_OBJECT_0 + RpTimer_Evt:
            RpTimerProc();
            break;
 
        case WAIT_OBJECT_0 + Queue_Evt:
            QueueProc();
            break;
 
        case WAIT_TIMEOUT:
            TimeOutProc();
            break;
        default:
            break;
        }
    }
    return;
}
 
void ConsumerThread::QueueProc()
{
    if(!HandleMsgs())
    {
        Sleep(1);
    }
}
 
void ConsumerThread::TimeOutProc()
{
    if (GetMsgBusStatus())
    {
        bool bTimeOut = false;
        if (!ConsumeMsg(1, 100, bTimeOut) && !bTimeOut)
        {
            MQTimerProc(); // Force to check connection
        }
    }
}
 
void ConsumerThread::MQInitialize()
{
    if(m_oRabbitMQ.ConnectMQ(GetMQHost(), GetMQPort(), GetLoginUser(), GetLoginPwd()))
    {
        TurnOnConfirmSelect(GetConfirmSelect());
        MQDeclareExchangeAndQueueConsumer();
    }
    else
    {
        SetMsgBusStatus(false);
    }
}
 
 
void ConsumerThread::MQDeclareExchangeAndQueueConsumer()
{
    if (m_oRabbitMQ.IsConfigError())
    {
        SetMsgBusStatus(false);
    }
    else
    {
        if(m_oRabbitMQ.DeclareExchage(GetExchangeUI(), GetExchangeType()))
        {
		if(m_oRabbitMQ.BindingQueue(GetQueueUI(), GetExchangeUI(), GetRoutingKey()))
            {
                if(!m_oRabbitMQ.hasConsumer())
                {
                    bool bResult = m_oRabbitMQ.DeclareConsumer(GetQueueUI());
                    SetMsgBusStatus(bResult);
                }
                else
                {
                    SetMsgBusStatus(true);
                }
            }
            else
            {
                SetMsgBusStatus(false);
            }
        }
        else
        {
            SetMsgBusStatus(false);
        }
    }
}
 
bool ConsumerThread::ConsumeMsg( DWORD dwMilliseconds, int nMsgCount, bool& bTimeOut )
{
    for (int i = 0; i < nMsgCount; i++)
    {
        MSG stMsg;
        if (NativeComsueMsg(stMsg, dwMilliseconds, bTimeOut))
        {
            bool retval = m_oMyQ.Push(stMsg);
            if (!retval)
            {
                stMsg.Free(); // Free the memory
                return false;
            }
        }
        else
        {
            // Might means no message to be consumed, just stop the counter, and let other event have chance to execute
            return false;
        }
    }
 
    return true;
}
 
void NativeComsueMsg(MSG& rMQMsg, DWORD dwMilliseconds, bool& bTimeOut) {
    amqp_rpc_reply_t reply;
    amqp_envelope_t envelope;
    amqp_maybe_release_buffers(m_con); // Avoid possible memory leak
    timeval m_time;
    m_time.tv_sec = dwMilliseconds/1000;
    m_time.tv_usec = (dwMilliseconds%1000)*1000;
    reply = amqp_consume_message(m_con, &envelope, &m_time, 0);//time out 1 second
    if (AMQP_RESPONSE_NORMAL != reply.reply_type)
    {
        bTimeOut = (
            (reply.reply_type == AMQP_RESPONSE_LIBRARY_EXCEPTION) &&
            (reply.library_error == AMQP_STATUS_TIMEOUT) );
        if (!bTimeOut)
        {
            ParseError(reply, CONSUMER, _T("Get message"));
        }
        return false;
    }
 
    bool bRet = false;
    amqp_pool_blocklist_t& rTheBody = envelope.message.pool.pages; //envelope.message.body;
    if (rTheBody.num_blocks > 0)
    {
        rMQMsg.pMQMsgBuf = new byte[rTheBody.num_blocks];
        if (NULL != rMQMsg.pMQMsgBuf)
        {
 
            ::memcpy(rMQMsg.pMQMsgBuf, rTheBody.blocklist, rTheBody.num_blocks);
            rMQMsg.dwMQMsgLen = (DWORD)rTheBody.num_blocks;
            bRet = true;
 
            LOGGER(_T("RabbitMQOperation::ConsumeMsg() Consume message from bus."));
        }
        else
        {
            LOGGER(_T("RabbitMQOperation::ConsumeMsg() NULL MSG."));
        }
    }
    else
    {
        LOGGER(_T("RabbitMQOperation::ConsumeMsg() Message Body is null."));
    }
 
    amqp_destroy_envelope(&envelope);
 
    return bRet;
} 
 
void ConsumerThread::CheckConnectionStatus()
{
    // do some sanity checks
    if (!m_oRabbitMQ.isConnected())
    {
        MQInitialize(); 
    }
    else
    {
        MQDeclareExchangeAndQueueConsumer();
    }
}
 
bool ConsumerThread::HandleMsgs( int nMsgCount /*= 100*/ )
{
    //handle message here
    return true;
}
