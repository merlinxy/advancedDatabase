from cassandra.cluster import Cluster


actor2filmed_in = {}
descriptions = []
genres_and_ratings = []
#### 1. parse the table
for line in open("imdb/movies_dump.txt"):
    year, title, rating, genres, actors = line[:-1].split("\t");
    if ((int(year)<2004) or (int(year)>2008)):
		  print year
		  continue
    """
    Here I upload all movies, but you should consider only those movies 
    that are inside your range of years.
    """
    title = title.replace("\'", " ")
    actors = [actor.strip() for actor in actors.split("|") if actor.strip()]
    actors = [actor.replace("\'", " ") for actor in actors]
    genres = [genre.strip() for genre in genres.split("|") if genre.strip() and len(genre) < 30]
    description = "TITLE: \"%s\"; YEAR: %s; RATING : %s; GENRES: %s; ACTORS: %s" %\
                                (title, year, rating, ",".join(genres), "|".join(actors))
    for actor in actors:
        actor2filmed_in.setdefault(actor, 0)
        actor2filmed_in[actor] += 1
    descriptions += [(title, description)]
    for genre in genres:
        genres_and_ratings += [(genre, float(rating), title)]
genres_and_ratings.sort()

cluster = Cluster(['54.185.30.189'])
session = cluster.connect()
#session.execute("USE example;")
session.execute("USE group15;")


session.execute("CREATE TABLE actors (name varchar PRIMARY KEY, filmed_in int);")
session.execute("CREATE TABLE popularity (fake_field int, filmed_in int, name varchar, PRIMARY KEY (fake_field, filmed_in, name));")

session.execute("CREATE TABLE ratings (genre varchar, rating float, title varchar, PRIMARY KEY (genre, rating, title));")
session.execute("CREATE TABLE movie_desc (title varchar PRIMARY KEY, description varchar);")

if 1 and "uploading actors":
    BATCH_SIZE = 10000
    print "uploading actors %d" % len(actor2filmed_in)
    actors2upload = [(actor, filmed_in) for actor, filmed_in in actor2filmed_in.items()]
    start_index = 0
    while start_index < len(actors2upload):
        if 1:
            data_chunk = actors2upload[start_index:start_index + BATCH_SIZE]
            chunk = ["INSERT INTO actors (name, filmed_in) VALUES ('%s', %d)" % \
                                                            (actor, filmed_in) for actor, filmed_in in data_chunk]
            chunk += ["INSERT INTO popularity (fake_field, name, filmed_in) VALUES (1, '%s', %d)" % \
                                                            (actor, filmed_in) for actor, filmed_in in data_chunk]
            command = "BEGIN BATCH %s \n APPLY BATCH;" % "\n".join(chunk)
            session.execute(command)
            print start_index, start_index + BATCH_SIZE, "done"
        start_index += BATCH_SIZE

if 1 and "uploading ratings":
    BATCH_SIZE = 4000
    print "uploading ratings %d" % (len(genres_and_ratings))
    start_index = 0
    while start_index < len(genres_and_ratings):
        if 1:
            chunk = genres_and_ratings[start_index:start_index + BATCH_SIZE]
            chunk = ["INSERT INTO ratings  (genre, rating, title) VALUES ('%s', %.3f, '%s')" % \
                      (genre, rating, title) for genre, rating, title in chunk]
            command = "BEGIN BATCH \n %s \n APPLY BATCH;" % "\n".join(chunk)
            #print command
            session.execute(command)
            print "\t", start_index, start_index + BATCH_SIZE, "done"
        start_index += BATCH_SIZE

if 1 and "uploading movies":
    BATCH_SIZE = 2000
    print "uploading movies %d" % (len(descriptions))
    start_index = 0
    while start_index < len(descriptions):
        if 1:
            chunk = descriptions[start_index:start_index + BATCH_SIZE]
            chunk = ["INSERT INTO movie_desc  (title, description) VALUES ('%s', '%s')" % \
                                        (genre, description) for genre, description in chunk]
            command = "BEGIN BATCH \n %s \n APPLY BATCH;" % "\n".join(chunk)
            #print command
            session.execute(command)
            print "\t", start_index, start_index + BATCH_SIZE, "done"
        start_index += BATCH_SIZE    


session.shutdown()
cluster.shutdown()        
