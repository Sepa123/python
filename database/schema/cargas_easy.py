def get_number(data : str):
    return [int(s) for s in str.split(data) if s.isdigit()][0]

def cargaEasy_schema(data_db):
    item = []
    # print(data_db[0][0])
    dictionary = {}
    dictionary["total"] = get_number(data_db[0][0])
    dictionary["verificado"] = get_number(data_db[1][0])
    item.append(dictionary)

    return item