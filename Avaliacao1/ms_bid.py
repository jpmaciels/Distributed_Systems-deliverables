"""
MS Lance (publisher e subscriber)
• Possui as chaves públicas de todos os clientes.
• (0,2) Escuta os eventos das filas lance_realizado, leilao_iniciado
e leilao_finalizado.
• (0,3) Recebe lances de usuários (ID do leilão; ID do usuário, valor
do lance) e checa a assinatura digital da mensagem utilizando a
chave pública correspondente. Somente aceitará o lance se:
o A assinatura for válida;
o ID do leilão existir e se o leilão estiver ativo;
o Se o lance for maior que o último lance registrado;
• (0,1) Se o lance for válido, o MS Lance publica o evento na fila
lance_validado.
• (0,2) Ao finalizar um leilão, deve publicar na fila leilao_vencedor,
informando o ID do leilão, o ID do vencedor do leilão e o valor
negociado. O vencedor é o que efetuou o maior lance válido até o
encerramento.
"""

import pika
import json
from loguru import logger
from typing import List


connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

class Auction:
    def __init__(self, auction_id, description, start_time, end_time, status):
        self.auction_id = auction_id
        self.description = description
        self.start_time = start_time
        self.end_time = end_time
        self.status = status
        self.bids = [] 
        self.highest_bid = 0.0
        self.highest_bidder = None


auctions: List[Auction] = []

def callback(ch, method, properties, body):
    logger.info(f"Received in routing key {method.routing_key}: \n\t\t{body}")

    if method.routing_key == '':
        # Fanout - auction started
        logger.info("Received auction started event")
        auction_data = json.loads(body)

        auction = Auction(
            auction_id=auction_data['id'],
            description=auction_data['description'],
            start_time=auction_data['start_time'],
            end_time=auction_data['end_time'],
            status=auction_data['status']
        )

        auctions.append(auction)
        logger.info(f"Auction created: {auction.auction_id} - {auction.description}")

    elif method.routing_key == 'bid_placed':
        logger.info("Received bid placed event")
        # body: {"auction_id": "123", "user_id": "456", "bid_amount": 100.0, "signature": "abc123"}

        # TODO
        # Only accepts if:
        # The signature is valid;
        # The auction ID exists and the auction is active;
        # The bid is higher than the last registered bid;

        # Publish to 'bid_validated' if valid

    elif method.routing_key == 'auction_ended':
        logger.info("Received auction ended event")
        # body: {"id": "123", "description": "Auction for item X", "start_time": "2023-10-01T10:00:00Z", "end_time": "2023-10-01T12:00:00Z", "status": "ended"}
        
        # TODO
        # publish to 'auction_winner' with the auction id, winner user id and winning bid amount.

    body_str = body.decode('utf-8')
    body_dict = json.loads(body_str)

    logger.info(f"Auction id: {body_dict.get('id')} \n description: {body_dict.get('description')} \n start_time: {body_dict.get('start_time')} \n end_time: {body_dict.get('end_time')} \n status: {body_dict.get('status')}")

def main():

    # Declare exchanges
    channel.exchange_declare(exchange='auction_fanout_exchange', exchange_type='fanout')
    channel.exchange_declare(exchange='direct_exchange', exchange_type='direct')

    # Declare queue
    channel.queue_declare(queue='ms_bid_queue')

    # bind queues to listen to
    channel.queue_bind(exchange='direct_exchange', queue='ms_bid_queue', routing_key='bid_placed')
    channel.queue_bind(exchange='direct_exchange', queue='ms_bid_queue', routing_key='auction_ended')
    channel.queue_bind(exchange='auction_fanout_exchange', queue='ms_bid_queue')

    channel.basic_consume(
        queue='ms_bid_queue', on_message_callback=callback, auto_ack=True)

    logger.info(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

if __name__ == "__main__":
    main()