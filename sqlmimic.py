import csv
import copy


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
        joined_data = copy.deepcopy(restaurants)
        for key, value in ratings.items():
            restaurant = joined_data[value["fk"]]
            restaurant["rating"] = value["rating"]
        result = joined_data
        self.result = None
        self.restaurants = restaurants
        self.ratings = ratings
        self.joined_data = joined_data
        self.tables = {"Restaurants": self.restaurants, "Ratings": self.ratings}
        # print(self.joined_data)
        # print(self.restaurants)

    def select(self, fields):
        if not fields:
            return {}
        query = []
        for key in self.joined_data:
            obj = {}
            for field in fields:
                obj[f"{field}"] = self.joined_data[key][f"{field}"]
            query.append(obj)
        self.result = query

    def fromm(self, tables):
        if not tables:
            return {}
        query = []
        for table in tables:
            if table in self.tables:
                query.append(self.tables[table])
        self.result = query

    def where(self, dictionary):
        query = []
        querykeys = list(dictionary.keys())
        queryvalues = list(dictionary.values())
        print(querykeys)
        print(queryvalues)
        for pk, restaurant in self.joined_data.items():
            for i in range(len(querykeys)):
                if restaurant[querykeys[i]] == queryvalues[i]:
                    if i == len(querykeys) - 1:
                        query.append(restaurant)
                else:
                    break
        self.result = query


class SQLOnDisk:
    def __init__(self):
        self.result = None
        self.journal = open("Journal.csv", "w")
        self.tables = ["Restaurants", "Ratings"]

    def select(self, fields):
        total_data = {}
        self.journal = open("Journal.csv", "r")
        csv_reader = csv.reader(self.journal)
        headers = next(csv_reader)
        # print(headers)
        indicies = []

        for field in fields:
            indicies.append(headers.index(field))
            # print(indicies)
            for i in range(len(indicies)):
                self.journal.seek(0)
                next(self.journal)
                data = []
                for line in csv_reader:
                    # print(line)
                    data.append(line[indicies[i]])
                total_data[field] = data

        # print(total_data)
        self.result = total_data

        # Write to csv
        fieldnames = list(self.result.keys())
        for data in total_data:
            with open("Journal.csv", "a") as f:
                csv_writer = csv.writer(f)
                csv_writer.writerow([data])
                csv_writer.writerow(total_data[data])

    def fromm(self, tables):
        # Build restaurants table as dict from csv
        tables_dict = {}
        for table in tables:
            data = {}
            table_name = str(table)
            csv_file = open(f"{table_name}.csv", "r")
            table_name_dict = csv.DictReader(csv_file)
            fields = [field.strip("\n") for field in csv_file.readline().split(",")]
            # print(fields)
            csv_file.seek(0)
            for line in table_name_dict:
                key = line["id"]
                data[key] = {field: line[field] for field in fields}
            tables_dict[f"{table_name}"] = data
        self.result = tables_dict

        # Write to csv
        for table in tables_dict:
            rows = tables_dict[table]
            fieldnames = [list(rows[row].keys()) for row in rows][0]
            csv_writer = csv.DictWriter(self.journal, fieldnames=fieldnames)
            csv_writer.writeheader()
            for row in rows:
                row = rows[row]
                csv_writer.writerow(row)

    def where(self, dictionary):
        data = {}
        keys = list(dictionary.keys())
        values = list(dictionary.values())
        print(keys)
        print(values)
        total_data = {}
        self.journal = open("Journal.csv", "r")
        csv_reader = csv.reader(self.journal)
        # print(headers)
        indicies = []
        for line in csv_reader:
            print(line)
            for key in keys:
                if line[0] in key:
                    data[key] = next(csv_reader)

        print(data)
        matches = []
        list1 = data[keys[0]]
        list2 = data[keys[1]]
        print(list1)
        print(list2)
        index1 = list1.index(values[0])
        index2 = list2.index(values[1])

        print(index1, index2)


# Tests
# sql = SQLInMemory()
# sql.select(["name", "rating"])
# print(sql.result)
# sql.fromm(["Restaurants", "Ratings"])
# print(sql.result)
# sql.where({"country": "Spain", "rating": "3"})
# print(sql.result)


sql = SQLOnDisk()
sql.fromm(["Restaurants"])
# print(sql.result)
sql.select(["name", "country"])
# print(sql.result)
sql.where({"name": "Asador Etxebarri", "country": "Spain"})
# print(sql.result)
