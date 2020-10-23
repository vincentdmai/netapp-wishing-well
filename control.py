import pika
import pymongo
import time
import sys

#TODO
#Change these later too
username = 'tom_swift'
password = 'flying_lab'

def rabbit_set_up(IP, PORT) :
    credentials = pika.PlainCredentials(username, password)

    #TODO
    #change localhost later to the repo IP
    #Replace the lower lines with these when we figure out the port and ip and credentials stuff
    #parameters = pika.ConnectionParameters(IP, PORT, '/', credentials)
    #connection = pika.BlockingConnection(parameters)
    #
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    print("[Ctrl 01] - Connecting to RabbitMQ instance on " + IP + " with port " + PORT)



    #Creating squires exchange
    channel.exchange_declare(exchange='Squires', exchange_type='direct')
    
    #Creating food queue and binding to squires exchange
    food_queue = channel.queue_declare(queue='Food', exclusive=True)
    channel.queue_bind(exchange='Squires', queue='Food', routing_key='Food')

    #Creating meetings queue and binding to squires exchange
    meetings_queue = channel.queue_declare(queue='Meetings', exclusive=True)
    channel.queue_bind(exchange='Squires', queue='Meetings', routing_key='Meetings')

    #Creating rooms queue and binding to squires exchange
    rooms_queue = channel.queue_declare(queue='Rooms', exclusive=True)
    channel.queue_bind(exchange='Squires', queue='Rooms', routing_key='Rooms')



    #Creating goodwin exchange
    channel.exchange_declare(exchange='Goodwin', exchange_type='direct')
    
    #Creating classrooms queue and binding to goodwin exchange
    classrooms_queue = channel.queue_declare(queue='Classrooms', exclusive=True)
    channel.queue_bind(exchange='Goodwin', queue='Classrooms', routing_key='Classrooms')

    #Creating auditorium queue and binding to goodwin exchange
    auditorium_queue = channel.queue_declare(queue='Auditorium', exclusive=True)
    channel.queue_bind(exchange='Goodwin', queue='Auditorium', routing_key='Auditorium')




    #Creating library exchange
    channel.exchange_declare(exchange='Library', exchange_type='direct')
    
    #Creating noise queue and binding to library exchange
    noise_queue = channel.queue_declare(queue='Noise', exclusive=True)
    channel.queue_bind(exchange='Library', queue='Noise', routing_key='Noise')

    #Creating seating queue and binding to library exchange
    seating_queue = channel.queue_declare(queue='Seating', exclusive=True)
    channel.queue_bind(exchange='Library', queue='Seating', routing_key='Seating')

    #Creating wishes queue and binding to library exchange
    wishes_queue = channel.queue_declare(queue='Wishes', exclusive=True)
    channel.queue_bind(exchange='Library', queue='Wishes', routing_key='Wishes')

    #TODO
    #Add print statement for queues and exchanges


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
                    repo_PORT = sys.argv[i+1]
            
        # If invalid parameter flags, then break with ERROR
        if not repo_IP or not repo_PORT:
            print('ERROR: INVALID ARGUMENT FLAGS. Specify -rip <REPOSITORY_IP> -rport <REPOSITORY_PORT>.')
            sys.exit()

    #set up rabbitMQ
    rabbit_set_up(repo_IP, repo_PORT)
    #set up mongoDB
    mongo_set_up()

    inp = ''
    while(inp != 'Exit') :
        inp = input("[Ctrl 04] -> Enter a command: ")

        if (inp == 'Exit') : 
            print("[Ctrl 08] - Exiting")
        else :
            first_split = inp.split(':')
            second_split = first_split[1].split('+')
            third_split = second_split[1].split(' ')

            action = first_split[0]
            place = second_split[0]
            subject = third_split[0]
            msgID = "23$" + str(time.time())

            client = pymongo.MongoClient()
            db = client[place]

            if (action == 'p') :
                message = third_split[1]

                post = {"Action": action,
                        "Place": place,
                        "MsgID": msgID,
                        "Subject": subject,
                        "Message": message}
                posts = db.posts
                post_id = posts.insert_one(post)

                #TODO
                #Change this message
                print("[Ctrl 05] – Inserted command into MongoDB: <MONGODB FORMAT INFO>")

                #TODO
                #Fix the print and add the produce functionality
                print("[Ctrl 06] - Produced message “<MESSAGE>” on <EXCHANGE:QUEUE>")
            elif (action == 'c') :
                #TODO
                #get the message from the consume so change this from the temp
                message = 'temp'

                post = {"Action": action,
                        "Place": place,
                        "MsgID": msgID,
                        "Subject": subject,
                        "Message": message}
                posts = db.posts
                post_id = posts.insert_one(post)

                #TODO
                #Change this message
                print("[Ctrl 05] – Inserted command into MongoDB: <MONGODB FORMAT INFO>")

                #TODO
                #Fix the print and add the consume functionality
                print("[Ctrl 07] - Consumed message “<MESSAGE>” on <EXCHANGE:QUEUE>")



    

