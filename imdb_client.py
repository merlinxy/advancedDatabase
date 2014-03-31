from cassandra.cluster import Cluster


class TClient(object):
    def __init__(self):
        self.cluster = Cluster(['54.185.30.189'])
        self.session = self.cluster.connect()
        # do not forget to switch into your key space!
        self.session.execute("use group15;")
        
    def get_movie(self, title):
        title = title.replace("'", " ")
        command = "select description from movie_desc WHERE title='%s';" % (title)
        descriptions = self.session.execute(command)
        if descriptions:
            description = descriptions[0][0]
        else:
            description = ""
        return description
    
    def get_top_movies(self, genre):
        command = "select title, rating from ratings WHERE genre='%s' ORDER BY rating DESC LIMIT 30;" % (genre)
        top_films = self.session.execute(command)
        top_films = [(title, rating) for title, rating in top_films]
        return top_films

    def get_top_actors(self):
        command = "select name, filmed_in from popularity WHERE fake_field=1 ORDER BY filmed_in DESC LIMIT 10;"
        top_actors = self.session.execute(command)
        top_actors = [(name, filmed_in) for name, filmed_in in top_actors]
        return top_actors
    
    def add_movie(self, title, year, rating, genres, actors):
        title = title.replace("\'", " ")
        # if the movie is in the database, drop exception
        if self.get_movie(title):
            raise Exception("the movie is in base")
        actors = [actor.replace("\'", " ") for actor in actors]
        genres = [genre.strip() for genre in genres if genre.strip() and len(genre) < 30]
        description = "TITLE: \"%s\"; YEAR: %d; RATING : %.3f; GENRES: %s; ACTORS: %s" %\
                                    (title, year, rating, ",".join(genres), "|".join(actors))
        insert_desc = "insert into movie_desc (title, description) values ('%s', '%s');"\
                                    % (title, description)
        insert_ratings = []
        for genre in genres:
            insert_ratings += ["insert into ratings (genre, rating, title) values ('%s', %.3f, '%s');" %\
                                                                   (genre, rating, title) ]
        insert_actors = []
        for name in actors:
            filmed_in = self.get_actor_filmed_in(name)
            if filmed_in:
                #remove the old record from the popularity
                command = "delete from popularity where fake_field=1 and filmed_in=%d and name='%s';" %\
                                                                   (filmed_in, name);
                insert_actors += [command]           
            #update filmed_in value in both tables
            filmed_in += 1
            command = "insert into actors (name, filmed_in) VALUES ('%s', %d);" % (name, filmed_in);
            insert_actors += [command]
            command = "insert into popularity (name, filmed_in, fake_field) VALUES ('%s', %d, 1);" %\
                                                                   (name, filmed_in);
            insert_actors += [command]
        all_commands = [insert_desc] + insert_ratings + insert_actors
        command = command = "BEGIN BATCH \n %s \n APPLY BATCH;" % "\n".join(all_commands)
        #print command
        self.session.execute(command)
        
    def delete_movie(self, title):
        title = title.replace("\'", " ")
        # if the movie is not in the database, drop exception
        movie_desc = self.get_movie(title)
        if not movie_desc:
            raise Exception("the movie is NOT in base")
        #parse desc
        genres = movie_desc[movie_desc.index("GENRES:") + len("GERNRES:"):\
                            movie_desc.index("ACTORS:")].strip()[:-1].split(",")
        actors = movie_desc[movie_desc.index("ACTORS:") + len("ACTORS:"):].strip().split("|")
        actors = [actor.strip() for actor in actors if actor.strip()]
        rating_str = movie_desc[movie_desc.index("RATING :") + len("RATING :") :\
                                          movie_desc.index("; GENRES")].strip()
        drop_desc = "delete from movie_desc where title='%s';" % (title)
        drop_ratings = []
        for genre in genres:
            drop_ratings += [ "delete from ratings where genre='%s' and rating=%s and title='%s';" %\
                                         (genre, rating_str, title) ]
        update_actors = []
        for name in actors:
            filmed_in = self.get_actor_filmed_in(name)
            if not filmed_in:
                #strange, let's drop an exception
                raise Exception("inconsistency between actors and movies")
            #remove the old record from the popularity
            command = "delete from popularity where fake_field=1 and filmed_in=%d and name='%s';" %\
                                         (filmed_in, name);
            update_actors += [command]
            #update filmed_in value in both tables
            filmed_in -= 1
            command = "insert into actors (name, filmed_in) VALUES ('%s', %d);" % (name, filmed_in);
            update_actors += [command]
            command = "insert into popularity (name, filmed_in, fake_field) VALUES ('%s', %d, 1);" %\
                                        (name, filmed_in);
            update_actors += [command]
        all_commands = [drop_desc] + drop_ratings + update_actors
        command = command = "BEGIN BATCH \n %s \n APPLY BATCH;" % "\n".join(all_commands)
        #print command
        self.session.execute(command)
    
    def get_actor_filmed_in(self, name):
        command = "select filmed_in from actors WHERE name='%s';" % (name)
        response = self.session.execute(command)
        filmed_in = response and response[0][0] or 0
        return filmed_in

    def __del__(self):
        self.session.shutdown()
        self.cluster.shutdown()
        


if __name__ == "__main__":
    client = TClient();
    print "connected"
    """ GET methods """
    print client.get_movie("From a Mess to the Masses (2010)")
    print "top movies:"
    for title, rating in client.get_top_movies("Action"):
        print "\t", title, rating
    print "top actors:"
    for name, filmed_in in client.get_top_actors():
        print "\t", name, filmed_in
    """ ADD/DELETE methods """
    print "ADD:"
    client.add_movie("Beta", 2016, 10.1, ["Action", "Drama"], ["Tester 1", "Tester 2", "Vernon, Dax"])
    print "movie:", client.get_movie("Beta")
    print "top:", client.get_top_movies("Action")[:5]
    print "actor filmed in:", client.get_actor_filmed_in("Tester 1")
    print "DELETE:"
    client.delete_movie("Beta")
    print "movie:", client.get_movie("Beta")
    print "top:", client.get_top_movies("Action")[:5]
    print "actor filmed in:", client.get_actor_filmed_in("Tester 1")
    try:
        del client
    except:
        pass
