import websocket
import json
import logging
import kafka 

list_symbols =["ltcusdt"]

producer = kafka.KafkaProducer(bootstrap_servers="localhost:9092")
TOPIC="test_init"

# Enable logging for websocket
websocket.enableTrace(True)

def on_message(ws, message):
  
    # message = bytearray(message.encode('utf-8'))
    # # Đảm bảo trade có cùng định dạng với schema của Kafka Consumer
    # producer.send('test_init', message)
    
    producer.send('test_init', json.dumps(json.loads(message)).encode('utf-8'))



def on_error(ws, error):
    print(error)

def on_close(ws):
    print("### closed ###")

def on_open(ws):
    # Subscribe to Binance WebSocket channels
    subscribe_msg = {
        "method": "SUBSCRIBE",
        "params": [item+"@trade" for item in list_symbols]
    }
    ws.send(json.dumps(subscribe_msg))

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    ws = websocket.WebSocketApp("wss://stream.binance.com:9443/ws",
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    ws.on_open = on_open

    ws.run_forever()
        
