from time import sleep,time
from paho.mqtt.client import Client
TEMP = 'temperature'
HUMIDITY = 'humidity'
NUMBERS = 'numbers'


def on_message(mqttc, data, msg):
    print (f'message:{msg.topic}:{msg.payload}:{data}')
    if data['status'] == 0:
        number = int(msg.payload) 
        if number%3 == 0:
            print(f'numero {number} multiplo de 3, nos suscribimos a temperatura')
            mqttc.publish('clients/multiplo3', msg.payload)
        elif number%5 == 0:
            print(f'numero {number} multiplo de 5 y no de 3, nos suscribimos a humedad')
            mqttc.publish('clients/multiplo5', msg.payload)
            mqttc.subscribe(TEMP)
            data['status'] = 1            
        else:
            print(f'numero {number} no es multiplo de 3 ni 5')
            mqttc.publish('clients/nomultiplo35', msg.payload)
            mqttc.subscribe(HUMIDITY)
            data['status'] = 2
    elif data['status'] == 1:
        if msg.topic==TEMP:
            temperature= int(msg.payload)
            if temperature>data['temp_threshold']:
                print(f'umbral temperatura {temperature} superado, cancelando suscripción')
                mqttc.unsubscribe(TEMP) # Esto debe ser lo último
                data['status'] = 0
    elif data['status'] == 2:
        if msg.topic==HUMIDITY:
            
            if msg.topic in 'clients/multiplo5':
                tiempo = time()
                humidity = int(msg.payload)
                mqttc.subscribe('clients/multiplo3')
                while time() - tiempo < humidity % 10:
                    print('recabando datos')
                mqttc.unsubscribe('clients/multiplo3')
                media = sum(data['num'])/len(data['num'])
                data['num'] = []
                mqttc.publish('clients/medias',f'La media en clients/multiplo3 desde la aparicion del {humidity} ha sido de {media}')
                print(f'cancelando suscripción en humedad')
                mqttc.unsubscribe(TEMP) 
                data['status'] = 0
            else:
                data['num'].append(float(msg.payload))
      
        
def on_log(mqttc, data, level, buf):
    print("LOG", data, level, buf)
    
def main(broker):
    data = {'temp_threshold':20,
            'humidity_threshold':80,
            'status': 0,
            'num' : []}
    mqttc = Client(userdata=data)
    mqttc.on_message = on_message
    mqttc.enable_logger()
    mqttc.connect(broker)
    mqttc.subscribe(f'{NUMBERS}/t1')
    mqttc.loop_forever()
    
if __name__ == "__main__":
    import sys
    if len(sys.argv)<2:
        print(f"Usage: {sys.argv[0]} broker")
        sys.exit(1)
    broker = sys.argv[1]
    main(broker)
