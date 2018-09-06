from datetime import datetime, timedelta


def date_generator(start, end):
    current = start
    while current < end:
        yield current
        current += timedelta(minutes=15)


#for t in date_generator(datetime(2006, 1, 1), datetime(2015, 1, 1)):
#    print t

#print (sum(1 for t in date_generator(datetime(2006, 1, 1), datetime(2015, 1, 1))) * 15)/1024.0/1024.0
