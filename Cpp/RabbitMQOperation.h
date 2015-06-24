#ifndef _RABBITMQOPERATION_H_
#define _RABBITMQOPERATION_H_

///////////////////////////////////////////////////////////////////////////////
//	 File: RabbitMQOperation.h
//	 Contents: Definition of the Rabbit MQ Operation
///////////////////////////////////////////////////////////////////////////////

// Attention: byte alignment correspondence with your project
#pragma pack(push)
#pragma pack(8)
#include <amqp_tcp_socket.h>
#include <amqp.h>
#include <amqp_framing.h>
#pragma pack(pop)


#include "RabbitMQMsg.h"
typedef amqp_connection_state_t Connection;
typedef amqp_socket_t Socket;
typedef amqp_rpc_reply_t RpcReply;

#define  CONNECTION_BROKEN          _T("Connection broken")
#define  NO_EXCHANGE                _T("No Exchange")
#define  NO_QUEUE                   _T("No Queue")
#define  EXCHANGE_CONFIG_MISMATCH   _T("Exchange Config Mismatch")
#define  QUEUE_CONFIG_MISMATCH      _T("Queue Config Mismatch")
#define  UNKNOWN_REASON             _T("Unknown Reason")

enum RabbitMQStatus
{
    CONNECTION = 0,
    CHANNEL = 1,
    EXCHANGE = 2,
    QUEUE = 3,
    CONSUMER = 4,
    PRODUCER = 5,
    Status_cnt,
};

class RabbitMQOperation{
public:
    RabbitMQOperation();
    virtual ~RabbitMQOperation();
public:
    bool IsConfigError();
    bool isConnected();
    bool ConnectMQ( LPCTSTR host, int port, LPCTSTR loginUser, LPCTSTR loginPwd);
    void DisConnectMQ();
    bool DeclareExchage(LPCTSTR exchangeName, LPCTSTR exchangeType);
    bool BindingQueue(LPCTSTR queueName, LPCTSTR exchangeName, LPCTSTR bindingKey);
    bool PublishMsg(LPCTSTR exchangeName, LPCTSTR routingKey, AGAMMQ_MESSAGE* pMsg, bool bConfirmSelect);
    bool DeclareConsumer( LPCTSTR lpszQueueName);
    bool ConsumeMsg(AGAMMQ_MESSAGE& rMQMsg, DWORD dwMilliseconds, bool& bTimeOut);
    bool hasConsumer();
    void TurnOnConfirmSelect(bool bConfirmSelect);
    bool ConfirmMessage();
private:
    void ParseError(char const *context);
    void ParseError(amqp_status_enum status, char const *context);
    void ParseError(amqp_rpc_reply_t reply, RabbitMQStatus status, char const *context);
    void SetStatus(RabbitMQStatus part, bool status);

	void ConsumeMsg_();

private:
    Socket * m_pSocket;
    Connection m_con;
    timeval m_Timeout;
    bool m_status[Status_cnt];
    bool m_isConfigError;

	amqp_bytes_t queuename;

public:
    unsigned int m_nConsumerCount;
    unsigned long m_nPendingMsg;
    TCHAR m_szStatusReason[64];
};

#endif /* _PRODUCERTHREAD_H_ */
