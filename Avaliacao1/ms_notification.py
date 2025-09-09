"""
MS Notificação (publisher e subscriber)
• (0,2) Escuta os eventos das filas lance_validado e
leilao_vencedor.
• (0,2) Publica esses eventos nas filas específicas para cada leilão,
de acordo com o seu ID (leilao_1, leilao_2, ...), de modo que
somente os consumidores interessados nesses leilões recebam as
notificações correspondentes.
"""
import pika
import json
import time
from datetime import datetime, timedelta
import threading
from loguru import logger

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

def handle_bid_validated(body):
    logger.info("Received bid validated event")
    # body: {"auction_id": "123", "user_id": "456", "bid_amount": 100.0, "signature": "abc123"}
    bid_data = json.loads(body)
    auction_queue = f"auction_{bid_data['auction_id']}"
    
    channel.queue_declare(queue=auction_queue)
    channel.basic_publish(
        exchange='direct_exchange',
        routing_key=auction_queue,
        body=json.dumps(bid_data)
    )
    logger.info(f" [x] Published bid validated to {auction_queue}: {bid_data}")

def handle_auction_winner(body):
    logger.info("Received auction winner event")
    # body: {"auction_id": "123", "winner_user_id": "456", "winning_bid_amount": 150.0}
    winner_data = json.loads(body)
    auction_queue = f"leilao_{winner_data['auction_id']}"
    
    channel.queue_declare(queue=auction_queue)
    channel.basic_publish(
        exchange='direct_exchange',
        routing_key=auction_queue,
        body=json.dumps(winner_data)
    )
    logger.info(f" [x] Published auction winner to {auction_queue}: {winner_data}")

def callback(ch, method, properties, body):
    logger.info(f"Received in routing key {method.routing_key}: \n\t\t{body}")

    logger.debug(f"\n\nch: {ch}\nmethod: {method}\nproperties: {properties}\nbody: {body}\n\n")

    if method.routing_key == 'bid_validated':
        handle_bid_validated(body)

    elif method.routing_key == 'auction_winner':
        handle_auction_winner(body)

def main():
    # Declare exchanges
    channel.exchange_declare(exchange='direct_exchange', exchange_type='direct')

    # Declare queue
    channel.queue_declare(queue='ms_notification_queue')

    # bind queues to listen to
    channel.queue_bind(exchange='direct_exchange', queue='ms_notification_queue', routing_key='bid_validated')
    channel.queue_bind(exchange='direct_exchange', queue='ms_notification_queue', routing_key='auction_winner')

    channel.basic_consume(
        queue='ms_notification_queue', on_message_callback=callback, auto_ack=True)

    logger.info(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

if __name__ == "__main__":
    main()