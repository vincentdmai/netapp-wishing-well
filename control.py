#!/usr/bin/python3
import pika
import pymongo
import time
import sys
import pprint
import copy

# Initialization of User Login
username = 'justmalvince'
password = 'team23'




def rabbit_set_up(IP, PORT) :
    credentials = pika.PlainCredentials(username, password)

    # Setting RabbitMQ connection based on credentials and IP/PORT
    parameters = pika.ConnectionParameters(IP, PORT, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    
    '''
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost'))
    '''
    
    channel = connection.channel()
    print("[Ctrl 01] - Connecting to RabbitMQ instance on " + IP + " with port " + str(PORT))
 
    listPairExQu = []
 
    #Creating squires exchange
    channel.exchange_declare(exchange='Squires', exchange_type='direct')
    
    #Creating food queue and binding to squires exchange
    channel.queue_declare(queue='Food', durable=True)
    channel.queue_bind(exchange='Squires', queue='Food', routing_key='Food')
    listPairExQu.append("Squires:Food")
 
    #Creating meetings queue and binding to squires exchange
    channel.queue_declare(queue='Meetings')
    channel.queue_bind(exchange='Squires', queue='Meetings', routing_key='Meetings')
    listPairExQu.append("Squires:Meetings")
 
    #Creating rooms queue and binding to squires exchange
    channel.queue_declare(queue='Rooms')
    channel.queue_bind(exchange='Squires', queue='Rooms', routing_key='Rooms')
    listPairExQu.append("Squires:Rooms")
 
    #Creating goodwin exchange
    channel.exchange_declare(exchange='Goodwin', exchange_type='direct')
    
    #Creating classrooms queue and binding to goodwin exchange
    channel.queue_declare(queue='Classrooms')
    channel.queue_bind(exchange='Goodwin', queue='Classrooms', routing_key='Classrooms')
    listPairExQu.append("Goodwin:Classrooms")
 
    #Creating auditorium queue and binding to goodwin exchange
    channel.queue_declare(queue='Auditorium')
    channel.queue_bind(exchange='Goodwin', queue='Auditorium', routing_key='Auditorium')
    listPairExQu.append("Goodwin:Auditorium")
 
    #Creating library exchange
    channel.exchange_declare(exchange='Library', exchange_type='direct')
    
    #Creating noise queue and binding to library exchange
    channel.queue_declare(queue='Noise')
    channel.queue_bind(exchange='Library', queue='Noise', routing_key='Noise')
    listPairExQu.append("Library:Noise")
 
    #Creating seating queue and binding to library exchange
    channel.queue_declare(queue='Seating')
    channel.queue_bind(exchange='Library', queue='Seating', routing_key='Seating')
    listPairExQu.append("Library:Seating")
 
    #Creating wishes queue and binding to library exchange
    channel.queue_declare(queue='Wishes')
    channel.queue_bind(exchange='Library', queue='Wishes', routing_key='Wishes')
    listPairExQu.append("Library:Wishes")
 
    #Print statement for queues and exchanges pairs
    print("[Ctrl 02] - Initialized Exchanges and Queues: ", listPairExQu)

    #Returning Channel to call later
    return channel


def mongo_set_up() :
    client = pymongo.MongoClient()

    squires_db = client['Squires']
    food = squires_db['food-collection']
    meetings = squires_db['meetings-collection']
    rooms = squires_db['rooms-collection']

    goodwin_db = client['Goodwin']
    classrooms = goodwin_db['classrooms-collection']
    auditorium = goodwin_db['auditorium-collection']

    library_db = client['Library']
    noise = library_db['noise-collection']
    seating = library_db['seating-collection']
    wishes = library_db['wishes-collection']

    print("[Ctrl 03] - Initialized MongoDB datastore")


# Callback Method for Channel Consuming 
def callback(ch, method, properties, body):
    
    if body:
        global CALLBACK_BODY
        CALLBACK_BODY = body.decode('ascii')
        ch.stop_consuming()
    


if __name__ == '__main__':
    # Parse the Command Line Server Info
    # Call Method that takes it in
    
    # Check correct number of parameters in command line
    if (len(sys.argv) != 5):
        print('ERROR: FAILURE TO RUN CONTROL')
        print('Invalid Number of Arguments. Specify -rip <REPOSITORY_IP> -rport <REPOSITORY_PORT>.')
        sys.exit()
    else:    
        # Iterate through the command line arguments and check if valid parameter flags
        for i, arg in enumerate(sys.argv):
            if (i + 1 < len(sys.argv)):
                if arg == '-rip':
                    repo_IP = sys.argv[i+1]
                elif arg == '-rport':
                    repo_PORT = int(sys.argv[i+1])
            
        # If invalid parameter flags, then break with ERROR
        if not repo_IP or not repo_PORT:
            print('ERROR: INVALID ARGUMENT FLAGS. Specify -rip <REPOSITORY_IP> -rport <REPOSITORY_PORT>.')
            sys.exit()

    #set up rabbitMQ
    channel = rabbit_set_up(repo_IP, repo_PORT)
    #set up mongoDB
    mongo_set_up()

    inp = ''
    while(inp.lower() != 'exit') :
        inp = input("[Ctrl 04] -> Enter a command: ")

        if (inp.lower() == 'exit') : 
            print("[Ctrl 08] - Exiting")
        else :
            first_split = inp.split(':')
            action = first_split[0]
            client = pymongo.MongoClient()
            if (action == 'p') :
                '''
                Cmd Line Parsing Logic
                '''
                second_split = first_split[1].split('+')
                third_split = second_split[1].split(' ')
                

                
                place = second_split[0]
                subject = third_split[0]
                msgID = "23$" + str(time.time())
                db = client[place]
                
                separator = ' '
                message = separator.join(third_split[1:])
                message = message.replace('"', '')
                '''
                End of CMD Line Parsing Logic
                '''

                post = {
                        'Action': action,
                        'Place': place,
                        'MsgID': msgID,
                        'Subject': subject,
                        'Message': message
                    }
                
                # Used copy to obtain a clean 'JSON' of post before it is modified by MongoDB through posts.insert_one()
                printPost = copy.copy(post)


                posts = db.posts
                post_id = posts.insert_one(post)

                # Printing Input Command String
                print("[Ctrl 05] – Inserted command into MongoDB: " + inp)

                # Printing Post in Pretty Print JSON format
                pprint.pprint(printPost, indent=2)
                
                # Establishes basic publish such that a body is 'published' within the specified queue for later consumption.
                channel.basic_publish(exchange = place, routing_key = subject, body=message)
                print("[Ctrl 06] - Produced message '" + message + "' on <" + place.upper() + ":" + subject.upper() + ">")
            
            elif (action == 'c') :
                '''
                Parsing CMD Line
                '''
                second_split = first_split[1].split('+')
                place = second_split[0]
                subject = second_split[1]
                msgID = "23$" + str(time.time())
                db = client[place]
                '''
                End of Parsing CMD Line
                '''

                post = {
                        'Action': action,
                        'Place': place,
                        'MsgID': msgID,
                        'Subject': subject
                    }

                # Used copy to obtain a clean 'JSON' of post before it is modified by MongoDB through posts.insert_one()
                printPost = copy.copy(post)
                posts = db.posts
                post_id = posts.insert_one(post)

                # Printing Input Command String
                print("[Ctrl 05] – Inserted command into MongoDB: " + inp)

                # Printing Post in Pretty Print JSON format
                pprint.pprint(printPost, indent=2)

                # Consume from RabbitMQ. Establishing basic consume connection and then accessing callback to obtain the message from
                # a specific queue.
                # The message is then printed out on the command line. 

                # Obtain current queue state extracted from the commandline to check if there are any pre-existing messages within the queue
                # If there is, access the basic consumption method to callback, else, do NOT consume since it will put the program into an infinite loop
                queue_state = channel.queue_declare(subject, durable=True, passive=True)
                queue_empty = queue_state.method.message_count == 0
                
                if not queue_empty:
                    channel.basic_consume(on_message_callback=callback, queue=subject, auto_ack=True)
                    channel.start_consuming()
                    print("[Ctrl 07] - Consumed message '" + CALLBACK_BODY + "' on <" + place.upper() + ":" + subject.upper() + ">")
                else:
                    print("[Ctrl 07] - No Messages in " + "<" + place.upper() + ":" + subject.upper() + ">")

    



    

