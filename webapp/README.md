To start Kafka:

```
cd demo-backend
docker-compose --log-level CRITICAL up
```

To start backend/host frontend:

```
mvn clean install spring-boot:run
```

React App:
The react app is now set to start both the java app and the react dev server, simply run

```
npm install (to get all the new dependencies)
npm start
```

Two URLs:

```
http://localhost:5656/transactions
http://localhost:5656/temerature
```

Swagger is available under:

```
http://localhost:5656/swagger-ui.html
```

Example SQL:

```
SELECT payeeId FROM source_table WHERE paymentAmount > 10
```


H2 Console:

```
URL: http://localhost:5656/h2-console/
```

| Setting      | Value              |
| ------------ | ------------------ |
| Driver Class | org.h2.Driver      |
| JDBC URL     | jdbc:h2:mem:testdb |
| User Name    | sa                 |
| Password     |                    |
