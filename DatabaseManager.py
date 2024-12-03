import mysql.connector
from prettytable import PrettyTable


class DatabaseManager:
    def __init__(self):
        self.host = "localhost"
        self.user = "root"
        self.password = "welcome1"
        self.database = "finalstore"

    def setup_database(self):
        print("Setting up the database...")

        conn = mysql.connector.connect(
            host=self.host,
            user=self.user,
            password=self.password
        )
        cursor = conn.cursor()

        try:
            # Create database
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.database}")
            conn.database = self.database

            # Create tables
            self.create_tables(cursor)

            # Insert sample data
            self.insert_sample_data(cursor)

            print("Database setup completed successfully.")
        except mysql.connector.Error as err:
            print(f"Error: {err}")
        finally:
            conn.commit()
            cursor.close()
            conn.close()

    def create_tables(self, cursor):
        table_queries = [
            """CREATE TABLE IF NOT EXISTS Customers (
                customer_id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255),
                email VARCHAR(255),
                phone VARCHAR(20),
                address TEXT,
                registration_date DATE
            )""",
            """CREATE TABLE IF NOT EXISTS Suppliers (
                supplier_id INT AUTO_INCREMENT PRIMARY KEY,
                supplier_name VARCHAR(255),
                contact_email VARCHAR(255),
                contact_phone VARCHAR(20),
                address TEXT
            )""",
            """CREATE TABLE IF NOT EXISTS Categories (
                category_id INT AUTO_INCREMENT PRIMARY KEY,
                category_name VARCHAR(255)
            )""",
            """CREATE TABLE IF NOT EXISTS Products (
                product_id INT AUTO_INCREMENT PRIMARY KEY,
                product_name VARCHAR(255),
                category_id INT,
                price DECIMAL(10, 2),
                stock_quantity INT,
                supplier_id INT,
                FOREIGN KEY (category_id) REFERENCES Categories(category_id),
                FOREIGN KEY (supplier_id) REFERENCES Suppliers(supplier_id)
            )""",
            """CREATE TABLE IF NOT EXISTS Orders (
                order_id INT AUTO_INCREMENT PRIMARY KEY,
                customer_id INT,
                order_date DATE,
                total_amount DECIMAL(10, 2),
                FOREIGN KEY (customer_id) REFERENCES Customers(customer_id)
            )""",
            """CREATE TABLE IF NOT EXISTS OrderDetails (
                order_detail_id INT AUTO_INCREMENT PRIMARY KEY,
                order_id INT,
                product_id INT,
                quantity INT,
                price_at_purchase DECIMAL(10, 2),
                FOREIGN KEY (order_id) REFERENCES Orders(order_id),
                FOREIGN KEY (product_id) REFERENCES Products(product_id)
            )""",
            """CREATE TABLE IF NOT EXISTS Payments (
                payment_id INT AUTO_INCREMENT PRIMARY KEY,
                order_id INT,
                payment_date DATE,
                payment_method VARCHAR(50),
                amount DECIMAL(10, 2),
                FOREIGN KEY (order_id) REFERENCES Orders(order_id)
            )"""
        ]

        for query in table_queries:
            cursor.execute(query)

    def insert_sample_data(self, cursor):
        data_queries = [
            # Insert Customers Data
            """INSERT INTO Customers (name, email, phone, address, registration_date) VALUES
            ('Alice Brown', 'alice@example.com', '1234567890', '123 Elm Street', '2024-01-15'),
            ('Bob Smith', 'bob@example.com', '1234567891', '456 Maple Avenue', '2024-02-20'),
            ('Charlie Davis', 'charlie@example.com', '1234567892', '789 Oak Lane', '2024-03-10'),
            ('Diana Green', 'diana@example.com', '1234567893', '101 Pine Road', '2024-01-25'),
            ('Ethan White', 'ethan@example.com', '1234567894', '202 Cedar Street', '2024-03-15'),
            ('Fiona Black', 'fiona@example.com', '1234567895', '303 Birch Avenue', '2024-02-10'),
            ('George Blue', 'george@example.com', '1234567896', '404 Walnut Drive', '2024-03-20'),
            ('Hannah Gold', 'hannah@example.com', '1234567897', '505 Chestnut Lane', '2024-01-05'),
            ('Ian Silver', 'ian@example.com', '1234567898', '606 Ash Street', '2024-02-25'),
            ('Julia Violet', 'julia@example.com', '1234567899', '707 Spruce Road', '2024-03-05')""",

            # Insert Suppliers Data
            """INSERT INTO Suppliers (supplier_name, contact_email, contact_phone, address) VALUES
            ('Tech Supplies Inc.', 'contact@techsupplies.com', '9876543210', '1 Tech Park'),
            ('Home Essentials Co.', 'support@homeessentials.com', '9876543211', '2 Home Street'),
            ('Office Depot', 'sales@officedepot.com', '9876543212', '3 Office Lane'),
            ('Green Gadgets', 'info@greengadgets.com', '9876543213', '4 Gadget Avenue'),
            ('Smart Electronics', 'support@smartelectronics.com', '9876543214', '5 Smart Road'),
            ('Kitchen Wonders', 'hello@kitchenwonders.com', '9876543215', '6 Kitchen Street'),
            ('Furniture Mart', 'contact@furnituremart.com', '9876543216', '7 Furniture Way'),
            ('Book Haven', 'sales@bookhaven.com', '9876543217', '8 Book Alley'),
            ('Fashion World', 'info@fashionworld.com', '9876543218', '9 Fashion Lane'),
            ('Toy Universe', 'support@toyuni.com', '9876543219', '10 Toy Street')""",
        ]

        for query in data_queries:
            cursor.execute(query)

    def execute_query(self, query, description):
        print(f"\n{description}")

        conn = mysql.connector.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database
        )
        cursor = conn.cursor()

        try:
            cursor.execute(query)
            rows = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]

            # Display results in a table
            table = PrettyTable()
            table.field_names = column_names
            for row in rows:
                table.add_row(row)

            print(table)
        except mysql.connector.Error as err:
            print(f"Error: {err}")
        finally:
            cursor.close()
            conn.close()

    @staticmethod
    def main():
        db_manager = DatabaseManager()
        db_manager.setup_database()

        # Execute Queries
        db_manager.execute_query(
            "SELECT c.name AS CustomerName, p.payment_method, p.amount "
            "FROM Payments p "
            "JOIN Orders o ON p.order_id = o.order_id "
            "JOIN Customers c ON o.customer_id = c.customer_id "
            "ORDER BY c.name ASC;",
            "List payments by customers in alphabetical order"
        )

        db_manager.execute_query(
            "SELECT name, email, registration_date "
            "FROM Customers "
            "WHERE registration_date >= DATE_SUB(CURDATE(), INTERVAL 1 YEAR);",
            "Find customers who registered within the past year"
        )

        db_manager.execute_query(
            "SELECT o.order_id, o.order_date, p.payment_method, p.amount "
            "FROM Orders o "
            "LEFT JOIN Payments p ON o.order_id = p.order_id;",
            "Display orders along with payment details"
        )

        db_manager.execute_query(
            "SELECT product_name, price "
            "FROM Products "
            "WHERE price BETWEEN 10 AND 100;",
            "List products with a price between $10 and $100"
        )

        db_manager.execute_query(
            "SELECT p.product_name, c.category_name "
            "FROM Products p "
            "JOIN Categories c ON p.category_id = c.category_id;",
            "Show products along with their category names"
        )

        db_manager.execute_query(
            "SELECT SUM(stock_quantity) AS TotalStockQuantity "
            "FROM Products;",
            "Calculate the total quantity of all products in stock"
        )

        db_manager.execute_query(
            "SELECT c.category_name, SUM(p.stock_quantity * p.price) AS TotalStockValue "
            "FROM Products p "
            "JOIN Categories c ON p.category_id = c.category_id "
            "GROUP BY c.category_name;",
            "Calculate the total value of unsold stock for each category"
        )

        db_manager.execute_query(
            "SELECT c.category_name, YEAR(o.order_date) AS Year, "
            "SUM(od.quantity * od.price_at_purchase) AS Revenue "
            "FROM Orders o "
            "JOIN OrderDetails od ON o.order_id = od.order_id "
            "JOIN Products p ON od.product_id = p.product_id "
            "JOIN Categories c ON p.category_id = c.category_id "
            "WHERE o.order_date >= DATE_SUB(CURDATE(), INTERVAL 1 YEAR) "
            "GROUP BY c.category_name, YEAR(o.order_date);",
            "Analyze annual revenue for each product category over the past 1 year"
        )

        db_manager.execute_query(
            "SELECT c.name AS CustomerName, SUM(o.total_amount) AS TotalSpending, "
            "AVG(o.total_amount) AS AverageOrderValue, COUNT(o.order_id) AS NumberOfOrders "
            "FROM Customers c "
            "JOIN Orders o ON c.customer_id = o.customer_id "
            "GROUP BY c.customer_id "
            "ORDER BY TotalSpending DESC;",
            "Rank customers based on total spending, average order value, and number of orders"
        )

        db_manager.execute_query(
            "SELECT c.name AS CustomerName, c.email "
            "FROM Customers c "
            "LEFT JOIN Orders o ON c.customer_id = o.customer_id "
            "WHERE o.order_date < DATE_SUB(CURDATE(), INTERVAL 6 MONTH) OR o.order_date IS NULL;",
            "Identify customers who haven't placed orders in the last 6 months and are at risk of churn"
        )

        db_manager.execute_query(
            "SELECT p.product_name, "
            "CEIL(SUM(od.quantity) / 60) AS ReorderQuantity "
            "FROM OrderDetails od "
            "JOIN Products p ON od.product_id = p.product_id "
            "GROUP BY p.product_id;",
            "Calculate how much stock needs to be reordered to meet demand for the next 60 days"
        )

        db_manager.execute_query(
            "SELECT od1.product_id AS Product1, od2.product_id AS Product2, COUNT(*) AS PairCount "
            "FROM OrderDetails od1 "
            "JOIN OrderDetails od2 ON od1.order_id = od2.order_id AND od1.product_id < od2.product_id "
            "GROUP BY od1.product_id, od2.product_id "
            "ORDER BY PairCount DESC;",
            "Identify product pairs frequently purchased together in the same order"
        )

        db_manager.execute_query(
            "SELECT p.product_name, "
            "CEIL(SUM(od.quantity) / 7) AS ReorderPoint "
            "FROM OrderDetails od "
            "JOIN Products p ON od.product_id = p.product_id "
            "GROUP BY p.product_id;",
            "Calculate the reorder point for each product based on sales trends and lead time"
        )

        db_manager.execute_query(
            "SELECT s.supplier_name, c.category_name, "
            "SUM(od.quantity * od.price_at_purchase) AS Revenue "
            "FROM OrderDetails od "
            "JOIN Products p ON od.product_id = p.product_id "
            "JOIN Categories c ON p.category_id = c.category_id "
            "JOIN Suppliers s ON p.supplier_id = s.supplier_id "
            "GROUP BY s.supplier_id, c.category_id;",
            "Determine how much revenue each supplier contributes to a specific product category"
        )

        db_manager.execute_query(
            "SELECT c.name AS CustomerName, COUNT(o.order_id) AS OrderCount "
            "FROM Customers c "
            "JOIN Orders o ON c.customer_id = o.customer_id "
            "GROUP BY c.customer_id "
            "HAVING OrderCount >= 10;",
            "Find 10 customers who placed orders for the same products"
        )

        db_manager.execute_query(
            "SELECT c.category_name, "
            "ROUND(SUM(od.quantity * od.price_at_purchase) / "
            "(SELECT SUM(od2.quantity * od2.price_at_purchase) FROM OrderDetails od2), 2) * 100 AS ContributionPercentage "
            "FROM OrderDetails od "
            "JOIN Products p ON od.product_id = p.product_id "
            "JOIN Categories c ON p.category_id = c.category_id "
            "GROUP BY c.category_id;",
            "Show each category's percentage contribution to the overall sales revenue"
        )

        db_manager.execute_query(
            "SELECT payment_method, COUNT(*) AS MethodCount "
            "FROM Payments "
            "GROUP BY payment_method "
            "ORDER BY MethodCount DESC;",
            "Determine the most popular payment method based on the number of payments"
        )

        db_manager.execute_query(
            "SELECT p.product_name, c.category_name, SUM(od.quantity) AS TotalQuantitySold, "
            "SUM(od.quantity * od.price_at_purchase) AS TotalRevenue "
            "FROM Products p "
            "JOIN Categories c ON p.category_id = c.category_id "
            "JOIN OrderDetails od ON p.product_id = od.product_id "
            "GROUP BY p.product_id;",
            "Create a sales report showing each product, its category, total quantity sold, and revenue generated"
        )

        db_manager.execute_query(
            "SELECT s.supplier_name, MIN(p.price) AS MinPrice "
            "FROM Suppliers s "
            "JOIN Products p ON s.supplier_id = p.supplier_id "
            "GROUP BY s.supplier_id;",
            "List suppliers who provide products with the minimum price in the database"
        )

        db_manager.execute_query(
            "SELECT c.category_name, SUM(od.quantity * od.price_at_purchase) AS TotalRevenue "
            "FROM Categories c "
            "JOIN Products p ON c.category_id = p.category_id "
            "JOIN OrderDetails od ON p.product_id = od.product_id "
            "GROUP BY c.category_id "
            "ORDER BY TotalRevenue DESC LIMIT 1;",
            "Determine which category has generated the highest total revenue"
        )

        print("\nApplication executed.")


if __name__ == "__main__":
    DatabaseManager.main()
