"""
Middleware orientado a Mensagens (MOM - Message Oriented
Middleware) responsável por organizar mensagens (eventos) em
filas, onde os produtores (publishers) as enviam e os
consumidores/assinantes (subscribers) as recebem.
"""
import pika

def connect_to_rabbitmq():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    return connection, channel

def declare_exchanges():
    connection, channel = connect_to_rabbitmq()
    channel.exchange_declare(exchange='auction_fanout_exchange', exchange_type='fanout')
    channel.exchange_declare(exchange='direct_exchange', exchange_type='direct')
    connection.close()

def main():
    declare_exchanges()

if __name__ == "__main__":
    main()