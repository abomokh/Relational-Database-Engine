import mysql.connector
from queries_db_script import execute_query, query_1, query_2, query_3, query_4, query_5

DOUBLE_BAR = "============================================================"
KEYWORD_1 = "adventures"
KEYWORD_2 = "Hiddleston"

def exe_query_1(keyword):
    print(DOUBLE_BAR)
    print("Executing Query 1:")
    print(query_1.__doc__)
    print(f"Results for keyword '{keyword}':")
    query = query_1(keyword)
    execute_query(query, True)
    print()
    
def exe_query_2(keyword):
    print(DOUBLE_BAR)
    print("Executing Query 2:")
    print(query_2.__doc__)
    print(f"Results for keyword '{keyword}':")
    query = query_2(keyword)
    execute_query(query, True)
    print()
    
def exe_query_3():
    print(DOUBLE_BAR)
    print("Executing Query 3:")
    print(query_3.__doc__)
    query = query_3()
    execute_query(query, True)
    print()
    
def exe_query_4():
    print(DOUBLE_BAR)
    print("Executing Query 4:")
    print(query_4.__doc__)
    query = query_4()
    execute_query(query, True)
    print()

def exe_query_5():
    print(DOUBLE_BAR)
    print("Executing Query 5:")
    print(query_5.__doc__)
    query = query_5()
    execute_query(query, True)
    print()

def main():
    try:
        exe_query_1(KEYWORD_1)
        exe_query_2(KEYWORD_2)
        exe_query_3()
        exe_query_4()
        exe_query_5()

    except mysql.connector.Error as err:
        print(f"Error: {err}")
if __name__ == "__main__":
    main()
