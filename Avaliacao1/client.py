'''Não  se  comunica  diretamente  com  nenhum  serviço,  toda  a 
comunicação é indireta através de filas de mensagens. 
•  (0,1)  Logo  ao  inicializar,  atuará  como  consumidor  recebendo 
eventos da fila leilao_iniciado. Os eventos recebidos contêm ID do 
leilão, descrição, data e hora de início e fim. 
•  (0,2) Possui um par de chaves pública/privada. Publica lances na 
fila  de  mensagens  lance_realizado.  Cada  lance  contém:  ID  do 
leilão, ID do usuário, valor do lance. O cliente assina digitalmente 
cada lance com sua chave privada. 
•  (0,2)  Ao  dar  um  lance  em  um  leilão,  o  cliente    atuará  como 
consumidor  desse  leilão,  registrando  interesse  em  receber 
notificações quando um novo lance for efetuado no leilão de seu 
interesse  ou  quando  o  leilão  for  encerrado.  Por  exemplo,  se  o 
cliente der um lance no leilão de ID 1, ele escutará a fila leilao_1.'''
import pika
import json
import uuid
import threading
from loguru import logger
from Crypto.PublicKey import RSA
from Crypto.Signature import pkcs1_15
from Crypto.Hash import SHA256
import base64
import os

#unique client id
CLIENT_ID = f"client_{uuid.uuid4().hex[:6]}"

# digital keys
private_key = RSA.generate(2048)
public_key = private_key.publickey()

subscribed_auctions = set()
lock = threading.Lock() 

def sign_message(message: dict) -> str:
    message_bytes = json.dumps(message, separators=(',', ':'), sort_keys=True).encode('utf-8')
    message_hash = SHA256.new(message_bytes)
    signature = pkcs1_15.new(private_key).sign(message_hash)
    return base64.b64encode(signature).decode('utf-8')

def message_listener():
    #listens for rabbit mq
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        channel = connection.channel()

        #queue for this client to receive all its messages
        result = channel.queue_declare(queue='', durable=False, exclusive=True, auto_delete=True)
        client_queue_name = result.method.queue

        #listen for auction start announcements; broadcast
        channel.queue_bind(exchange='auction_fanout_exchange', queue=client_queue_name)

        logger.info(f"[{CLIENT_ID}] Listening on queue '{client_queue_name}' for auction announcements.")

        def callback(ch, method, properties, body):
            #process incoming messages
            message = json.loads(body)
            
            if method.exchange == 'auction_fanout_exchange':
                logger.info(f"New Auction Started: ID={message['id']}, Description='{message['description']}'")
            
            elif 'winner_user_id' in message:
                winner_id = message['winner_user_id']
                auction_id = message['auction_id']
                amount = message['winning_bid_amount']
                
                if winner_id == CLIENT_ID:
                    logger.success(f"YOU WON auction '{auction_id}' with a bid of ${amount:.2f}!")
                else:
                    logger.warning(f"You did not win auction '{auction_id}'. Winner: {winner_id} with ${amount:.2f}.")

            #if someone bided in an auction this client is subscribed to
            else:
                 logger.info(f"New Validated Bid: Auction={message['auction_id']}, User={message['user_id']}, Amount=${message['bid_amount']:.2f}")
            
            print("\n> Enter [auction_id] [amount] to place a bid: ", end="")


        channel.basic_consume(queue=client_queue_name, on_message_callback=callback, auto_ack=True)
        
        while True:
            with lock:
                auctions_to_check = subscribed_auctions.copy()

            for auction_id in auctions_to_check:
                #one for validated bids and another for the winner.
                validated_bid_key = f"auction_{auction_id}"
                winner_key = f"leilao_{auction_id}"
                
                channel.queue_bind(exchange='direct_exchange', queue=client_queue_name, routing_key=validated_bid_key)
                channel.queue_bind(exchange='direct_exchange', queue=client_queue_name, routing_key=winner_key)
                logger.info(f"[{CLIENT_ID}] Subscribed to receive notifications for auction '{auction_id}'.")
            
            with lock:
                subscribed_auctions.difference_update(auctions_to_check)

            connection.sleep(1)

    except pika.exceptions.AMQPConnectionError as e:
        logger.error(f"Could not connect to RabbitMQ. Please ensure it is running. Error: {e}")
    except Exception as e:
        logger.error(f"An error occurred in the listener thread: {e}")


def main():
    logger.info(f"Client started with ID: {CLIENT_ID}")

    with open(f"keys/{CLIENT_ID}_public.pem", "wb") as f:       
        f.write(public_key.export_key('PEM'))
        logger.info(f"Public key saved to keys/{CLIENT_ID}_public.pem")

    listener_thread = threading.Thread(target=message_listener, daemon=True)
    listener_thread.start()

    threading.Event().wait(2)

    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        channel = connection.channel()
        
        while True:
            try:
                # Prompt for user input
                user_input = input("\n> Enter <auction_id> <amount> to place a bid: ")

                if user_input.lower() == 'exit':
                    break

                parts = user_input.split()
                if len(parts) != 2:
                    logger.warning("Invalid input. Please use the format: <auction_id> <amount>")
                    continue

                auction_id = parts[0]
                bid_amount = float(parts[1])

                bid_message = {
                    "auction_id": auction_id,
                    "user_id": CLIENT_ID,
                    "bid_amount": bid_amount,
                }
                
                signature = sign_message(bid_message)
                bid_message_with_signature = bid_message.copy()
                bid_message_with_signature['signature'] = signature

                channel.basic_publish(
                    exchange='direct_exchange',
                    routing_key='bid_placed', 
                    body=json.dumps(bid_message_with_signature)
                )

                logger.success(f"Bid of ${bid_amount:.2f} sent for auction '{auction_id}'.")

                with lock:
                    subscribed_auctions.add(auction_id)

            except ValueError:
                logger.warning("Invalid bid amount. Please enter a number.")
            except pika.exceptions.AMQPConnectionError as e:
                logger.error(f"Connection error while sending bid: {e}")
                break
            except Exception as e:
                logger.error(f"An error occurred: {e}")

    finally:
        if 'connection' in locals() and connection.is_open:
            connection.close()
        logger.info("Client shut down.")

        #delete public key file on exit
        try:
            os.remove(f"keys/{CLIENT_ID}_public.pem")
            logger.info(f"Public key file keys/{CLIENT_ID}_public.pem deleted.")
        except OSError as e:
            logger.error(f"Error deleting public key file: {e}")


if __name__ == "__main__":
    main()