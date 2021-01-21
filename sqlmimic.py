import csv


class SQLInMemory:
    def __init__(self):
        # Build restaurants table as dict from csv
        restaurants_csv_file = open("Restaurants.csv", "r")
        restaurants_dict = csv.DictReader(restaurants_csv_file)
        restaurants = {}
        fieldnames = ["name", "country"]
        for line in restaurants_dict:
            key = line["id"]
            restaurants[key] = {field: line[field] for field in fieldnames}
        # print("Restaurants: \n", restaurants)

        # Build ratings table as dict from csv
        ratings_csv_file = open("Ratings.csv", "r")
        ratings_dict = csv.DictReader(ratings_csv_file)
        ratings = {}
        fieldnames = ["fk", "rating"]
        for line in ratings_dict:
            key = line["id"]
            ratings[key] = {field: line[field] for field in fieldnames}
        # print("Ratings: \n", ratings)

        # Joining the tables into 1 dictionary
        for key, value in ratings.items():
            restaurant = restaurants[value["fk"]]
            restaurant["rating"] = value["rating"]
        result = restaurants
        self.result = result
        self.restaurants = restaurants
        self.ratings = ratings
        self.tables = {"restaurants": self.restaurants, "ratings": self.ratings}
        # print(self.result)

    def select(self, fields):
        if not fields:
            return {}
        query = []
        for key in self.result:
            obj = {}
            for field in fields:
                obj[f"{field}"] = self.result[key][f"{field}"]
            query.append(obj)
        return query

    def fromm(self, tables):
        if not tables:
            return {}
        query = []
        for table in tables:
            if table in self.tables:
                query.append(self.tables[table])
        return query

    def where(self, dictionary):
        query = []
        querykeys = list(dictionary.keys())
        queryvalues = list(dictionary.values())
        print(querykeys)
        print(queryvalues)
        for pk, restaurant in self.restaurants.items():
            for i in range(len(querykeys)):
                if restaurant[querykeys[i]] == queryvalues[i]:
                    if i == len(querykeys) - 1:
                        query.append(restaurant)
                else:
                    break
        return query


class SQLOnDisk:
    def __init__(self):
        self.result = None

    def select(self, fields):
        sql = SQLInMemory().select(fields)
        self.result = sql
        with open("Journal.csv", "w") as new_file:

            print(f"sql: {sql}")
            for item in sql:
                print(item)
                new_file.write(str(item) + ",\n")

    def fromm(self, tables):
        sql = SQLInMemory().fromm(tables)
        self.result = sql
        with open("Journal.csv", "w") as new_file:

            print(f"sql: {sql}")
            for item in sql:
                print(item)
                new_file.write(str(item) + ",\n")

    def where(self, dictionary):
        sql = SQLInMemory().where(dictionary)
        self.result = sql
        with open("Journal.csv", "w") as new_file:

            print(f"sql: {sql}")
            for item in sql:
                print(item)
                new_file.write(str(item) + ",\n")


Tests
sql = SQLInMemory()
print(sql.select(fields=["name", "rating"]))
print()
print(sql.fromm(tables=["restaurants", "ratings"]))
print()
print(sql.where(dictionary={"country": "Spain", "rating": "3"}))
print()


# sql = SQLOnDisk()
# print(sql.select(fields=["name", "rating"]))
# print()
# print(sql.fromm(tables=["restaurants", "ratings"]))
# print()
# print(sql.where(dictionary={"country": "Spain", "rating": "3"}))
# print()
