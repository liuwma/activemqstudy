package pers.lwm.activemqstudy;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;

public class QueueDemo implements ExceptionListener {

	public static void main(String[] args) throws JMSException {

		//new QueueDemo().producerMsg();
		
		new QueueDemo().consumeMsg();
	}

	public void producerMsg() throws JMSException {
		// Create a ConnectionFactory，创建连接工厂
		ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://127.0.0.1:61616");

		// Create a Connection，创建连接
		Connection connection = connectionFactory.createConnection();
		connection.start();// 打开连接

		// Create a Session//创建会话
		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);// 指定ACK_Mode签收确认模式为自动确认

		// Create the destination (Topic or Queue)
		Destination destination = session.createQueue("TEST.FOO");// 创建消息目标(点对点模型队列)
		// Destination destination =
		// session.createTopic("TEST.FOO");//创建消息目标(订阅主题)
		// Create a MessageProducer from the Session to the Topic or
		// Queue,创建消息生产者
		MessageProducer producer = session.createProducer(destination);// 创建消息生产者
		producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);// 指定传输模式-非持久性消息

		// Create a messages，创建消息
		String text = "Hello world! From: " + Thread.currentThread().getName() + " : " + this.hashCode();
		TextMessage message = session.createTextMessage(text);// 创建文本消息

		// Tell the producer to send the message
		System.out.println("Sent message: " + message.hashCode() + " : " + Thread.currentThread().getName());
		producer.send(message);// 发送消息

		// Clean up
		session.close();// 关闭会话
	}

	public void consumeMsg() throws JMSException {
		// Create a ConnectionFactory，创建连接工厂
		ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://127.0.0.1:61616");

		// Create a Connection，创建连接
		Connection connection = connectionFactory.createConnection();
		connection.start();// 打开连接

		connection.setExceptionListener(this);// 指定连接使用的异常监听器

		// Create a Session，创建会话
		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE); // 指定ACK_Mode签收确认模式为自动确认

		// Create the destination (Topic or Queue)
		Destination destination = session.createQueue("TEST.FOO");// 创建消息目标(点对点模型队列)
		// Destination destination =
		// session.createTopic("TEST.FOO");//创建消息目标(订阅主题)

		// Create a MessageConsumer from the Session to the Topic or
		// Queue//创建消息消费者
		MessageConsumer consumer = session.createConsumer(destination);

		// Wait for a message
		Message message = consumer.receive(1000);// 接收1000毫秒内到达的消息，如果没有收到此方法将阻塞等待直到指定超时时间

		if (message instanceof TextMessage) {// 判断消息类型是否为文本消息
			TextMessage textMessage = (TextMessage) message;
			String text = textMessage.getText();
			System.out.println("Received: " + text);
		} else {
			System.out.println("Received: " + message);
		}

		consumer.close();// 关闭消费者
		session.close();// 关闭会话
		connection.close();// 关闭连接
	}

	@Override
	public void onException(JMSException arg0) {
		
		
	}
}
