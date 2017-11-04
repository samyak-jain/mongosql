from pymongo import MongoClient
import sqlite3
import datetime
from datetime import timezone

def mongo_status():
    client = MongoClient()
    db = client.test

    posts = db.posts
    x = list(posts.find({}))
    return x

def mongo_time():
    x = mongo_status()
    x = [i["_id"] for i in x]
    # print(x)
    z = sorted(x, key=lambda y: y.generation_time)
    return z[-1].generation_time


def sql_status():
    conn = sqlite3.connect("example.db")
    c = conn.cursor()
    c.execute("SELECT datetime(timestamp, 'localtime') FROM test ORDER BY timestamp DESC")
    return datetime.datetime.strptime(c.fetchall()[0][0], "%Y-%m-%d %H:%M:%S")


def mongo_insert(fname = "name1", lname = "name2"):
    client = MongoClient()
    db = client.test

    posts = db.posts
    for i in range(1000):
        posts.insert_one({"fname": fname, "lname": lname})

def sql_insert(fname = "name1", lname = "name2"):
    conn = sqlite3.connect("example.db")
    c = conn.cursor()
    for i in range(1000):
        c.execute("INSERT INTO test(fname, lname) VALUES (?, ?)",(fname, lname))

    c.execute("SELECT * FROM test")
    # for i in c.fetchall():
        # print(i)

    conn.commit()

    conn.close()

def mongo_update(fname = "name3", lname = "name4"):
    client = MongoClient()
    db = client.test

    posts = db.posts

    resp = posts.aggregate([{"$sample": {"size": 5}}])

    for i in resp:
        x = i["_id"]
        posts.update_one({"_id": x}, {"$set": {"fname": fname, "lname": lname}}, upsert=False)

    # print(list(posts.find()))


def sql_update():
    conn = sqlite3.connect("example.db")
    c = conn.cursor()
    c.execute("SELECT ROWID FROM test ORDER BY RANDOM() LIMIT 5")
    x  = c.fetchall()
    for i in x:
        c.execute("UPDATE test SET fname=?, lname=? WHERE ROWID=?", ("name2", "name3", i[0]))

    c.execute("SELECT * FROM test")
    y = c.fetchall()

    # for i in y:
        # print(i)

    conn.commit()

    conn.close()


def mongo_delete():
    client = MongoClient()
    db = client.test

    posts = db.posts

    resp = posts.aggregate([{"$sample": {"size": 5}}])

    for i in resp:
        x = i["_id"]
        posts.delete_one({"_id": x})

    # print(list(posts.find()))


def sql_delete():
    conn = sqlite3.connect("example.db")
    c = conn.cursor()
    c.execute("SELECT ROWID FROM test ORDER BY RANDOM() LIMIT 5")
    x = c.fetchall()
    for i in x:
        c.execute("DELETE FROM test WHERE ROWID=?", i)

    conn.commit()

    conn.close()



# mongo_status()

# for i in range(10):
    # mongo_insert()

# mongo_update()

# sql_insert()

# sql_update()

# mongo_delete()

# sql_delete()

def timeit(func, db):

    time1 = datetime.datetime.now()
    time = datetime.datetime.now(timezone.utc)
    func()
    if (db == 0):
        print("Mongo")
        return time - mongo_time() + datetime.timedelta(milliseconds=50)
    else:
        print("SQL")
        # print(time1)
        # print(sql_status())
        return (time1 - sql_status())


def main():
    print("Insertion: ")
    print(timeit(mongo_insert, 0))
    print(timeit(sql_insert, 1))
    print("Updation: ")
    print(timeit(mongo_update, 0))
    print(timeit(sql_update, 1))
    print("Deletion: ")
    print(timeit(mongo_delete, 0))
    print(timeit(sql_delete, 1))


if __name__=="__main__":
    main()
