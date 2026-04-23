# STEP 0

# SQL Library and Pandas Library
import sqlite3
import pandas as pd

# Connect to the database
conn = sqlite3.connect('data.sqlite')

pd.read_sql("""SELECT * FROM sqlite_master""", conn)

# STEP 1
# Return first and last names and job titles for all employees in Boston
df_boston = pd.read_sql("""
    SELECT e.firstName, e.lastName, e.jobTitle
    FROM employees e
    JOIN offices o ON e.officeCode = o.officeCode
    WHERE o.city = 'Boston'
""", conn)

# STEP 2
# Are there any offices that have zero employees?
df_zero_emp = pd.read_sql("""
    SELECT o.officeCode, o.city
    FROM offices o
    LEFT JOIN employees e ON o.officeCode = e.officeCode
    GROUP BY o.officeCode, o.city
    HAVING COUNT(e.employeeNumber) = 0
""", conn)

# STEP 3
# Return employees' first name, last name, city, state of office (include all employees)
df_employee = pd.read_sql("""
    SELECT e.firstName, e.lastName, o.city, o.state
    FROM employees e
    LEFT JOIN offices o ON e.officeCode = o.officeCode
    ORDER BY e.firstName, e.lastName
""", conn)

# STEP 4
# Return customers who have not placed any orders (contact info and sales rep)
df_contacts = pd.read_sql("""
    SELECT c.contactFirstName, c.contactLastName, c.phone, c.salesRepEmployeeNumber
    FROM customers c
    LEFT JOIN orders o ON c.customerNumber = o.customerNumber
    WHERE o.orderNumber IS NULL
    ORDER BY c.contactLastName
""", conn)

# STEP 5
# Customer payments with amounts sorted descending (using CAST for numeric sorting)
df_payment = pd.read_sql("""
    SELECT c.contactFirstName, c.contactLastName, p.amount, p.paymentDate
    FROM customers c
    JOIN payments p ON c.customerNumber = p.customerNumber
    ORDER BY CAST(p.amount AS FLOAT) DESC
""", conn)

# STEP 6
# Employees whose customers have average credit limit over 90k (top 4 by customer count)
df_credit = pd.read_sql("""
    SELECT e.employeeNumber, e.firstName, e.lastName, COUNT(c.customerNumber) AS num_customers
    FROM employees e
    JOIN customers c ON e.employeeNumber = c.salesRepEmployeeNumber
    GROUP BY e.employeeNumber, e.firstName, e.lastName
    HAVING AVG(c.creditLimit) > 90000
    ORDER BY num_customers DESC
    LIMIT 4
""", conn)

# STEP 7
# Product name, number of orders, and total units sold (sorted by total units highest to lowest)
df_product_sold = pd.read_sql("""
    SELECT p.productName, COUNT(od.orderNumber) AS numorders, SUM(od.quantityOrdered) AS totalunits
    FROM products p
    JOIN orderdetails od ON p.productCode = od.productCode
    GROUP BY p.productName
    ORDER BY totalunits DESC
""", conn)

# STEP 8
# Product name, code, and number of unique customers who ordered each product
df_total_customers = pd.read_sql("""
    SELECT p.productName, p.productCode, COUNT(DISTINCT o.customerNumber) AS numpurchasers
    FROM products p
    JOIN orderdetails od ON p.productCode = od.productCode
    JOIN orders o ON od.orderNumber = o.orderNumber
    GROUP BY p.productName, p.productCode
    ORDER BY numpurchasers DESC
""", conn)

# STEP 9
# Number of customers per office (office code and city)
df_customers = pd.read_sql("""
    SELECT o.officeCode, o.city, COUNT(c.customerNumber) AS n_customers
    FROM offices o
    JOIN employees e ON o.officeCode = e.officeCode
    JOIN customers c ON e.employeeNumber = c.salesRepEmployeeNumber
    GROUP BY o.officeCode, o.city
""", conn)

# STEP 10
# Employees who sold products ordered by fewer than 20 customers (using subquery)
df_under_20 = pd.read_sql("""
    SELECT DISTINCT e.employeeNumber, e.firstName, e.lastName, o.city, e.officeCode
    FROM employees e
    JOIN customers c ON e.employeeNumber = c.salesRepEmployeeNumber
    JOIN orders ord ON c.customerNumber = ord.customerNumber
    JOIN orderdetails od ON ord.orderNumber = od.orderNumber
    JOIN products p ON od.productCode = p.productCode
    WHERE p.productCode IN (
        SELECT p2.productCode
        FROM products p2
        JOIN orderdetails od2 ON p2.productCode = od2.productCode
        JOIN orders ord2 ON od2.orderNumber = ord2.orderNumber
        GROUP BY p2.productCode
        HAVING COUNT(DISTINCT ord2.customerNumber) < 20
    )
""", conn)

# Close the connection
conn.close()

# Optional: Print confirmation
print("All queries completed successfully!")
print(f"STEP 1 - Boston employees: {len(df_boston)}")
print(f"STEP 2 - Offices with no employees: {len(df_zero_emp)}")
print(f"STEP 3 - All employees: {len(df_employee)}")
print(f"STEP 4 - Customers with no orders: {len(df_contacts)}")
print(f"STEP 5 - Payment records: {len(df_payment)}")
print(f"STEP 6 - Top credit employees: {len(df_credit)}")
print(f"STEP 7 - Products sold: {len(df_product_sold)}")
print(f"STEP 8 - Products with customer counts: {len(df_total_customers)}")
print(f"STEP 9 - Offices with customers: {len(df_customers)}")
print(f"STEP 10 - Employees from subquery: {len(df_under_20)}")