import pyodbc
import pymysql


def build_sql_connections(creds):
    db_type = creds.get("db_type", "sqlserver").lower()

    if db_type == "sqlserver":
        driver = creds.get("driver", "ODBC Driver 18 for SQL Server")
        server = creds["server"]
        database = creds["database"]
        auth_type = creds["auth_type"].lower()

        if auth_type == "sql":
            conn_str = (
                f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};"
                f"UID={creds['username']};PWD={creds['password']};"
                "Encrypt=yes;TrustServerCertificate=yes;"
            )

        elif auth_type == "azure_ad_password":
            conn_str = (
                f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};"
                "Authentication=ActiveDirectoryPassword;"
                f"UID={creds['username']};PWD={creds['password']};"
            )

        elif auth_type == "managed_identity":
            conn_str = (
                f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};"
                "Authentication=ActiveDirectoryMsi;"
            )

        else:
            raise Exception("Invalid SQL Server Auth Type")

        return pyodbc.connect(conn_str, timeout=20)

    elif db_type == "mysql":
        return pymysql.connect(
            host=creds["server"],
            user=creds["username"],
            password=creds["password"],
            database=creds["database"],
            port=int(creds.get("port", 3306)),
            connect_timeout=20
        )


    else:
        raise Exception("Unsupported database type")
