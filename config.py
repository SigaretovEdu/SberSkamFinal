host="localhost"
user="admin"
password="root"
db_name="postgres"
port = 5432

init_command = """
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id CHARACTER VARYING (25) PRIMARY KEY,
    date CHARACTER VARYING (25),
    card CHARACTER VARYING (25),
    account CHARACTER VARYING (25),
    account_valid_to CHARACTER VARYING (25),
    client CHARACTER VARYING (10),
    last_name CHARACTER VARYING (25),
    first_name CHARACTER VARYING (25),
    patronymic CHARACTER VARYING (25),
    date_of_birth CHARACTER VARYING (25),
    passport BIGINT,
    passport_valid_to CHARACTER VARYING (25),
    phone CHARACTER VARYING (15),
    oper_type CHARACTER VARYING (15),
    amount REAL,
    oper_result CHARACTER VARYING (15),
    terminal CHARACTER VARYING (15),
    terminal_type CHARACTER VARYING (10),
    city CHARACTER VARYING (25),
    address CHARACTER VARYING (100)
);"""

add_command="""
    INSERT INTO transactions VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
"""

# (transaction_id,date,card,account,account_valid_to,client,last_name,first_name,patronymic,date_of_birth,passport,passport_valid_to,phone,oper_type,amount,oper_result,terminal,terminal_type,city,address)
#     arr=("459270924","2020-05-01T00:00:29","59649132026167121328","40817810000001139973","2036-01-16T00:00:00","3-95179","Ебланов", "Еблан","Ебланович","1938-06-25T00:00:00",7076445954,"2022-11-09T00:00:00","+79497481039","умом",31576.6,"выполнено","POS43792","POS","Одинцово","Маковского,2")