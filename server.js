import { DBSQLClient } from '@databricks/sql';
import express from 'express';
import dotenv from 'dotenv';
import cors from 'cors';
import compression from 'compression';

const app = express();
dotenv.config();
const port = process.env.PORT || 3010;

app.use(express.json());
app.use(cors());
app.use(compression());

const token = process.env.DATABRICKS_TOKEN || "dapie6b8d05bc18929b4e120930c1152ed71-3";
const server_hostname = process.env.DATABRICKS_HOST || "adb-4821506742419671.11.azuredatabricks.net";
const http_path = process.env.DATABRICKS_PATH || "/sql/1.0/warehouses/a45ce58754c146b6";

const client = new DBSQLClient();
let clientConnection; // Reutilizar la conexión

const connectToDatabricks = async () => {
  if (!clientConnection) {
    try {
      clientConnection = await client.connect({
        token: token,
        host: server_hostname,
        path: http_path,
      });
      console.log('Connected to Databricks');
    } catch (error) {
      console.error("Error connecting to Databricks:", error);
      throw error;
    }
  }
  return clientConnection;
};

const executeQuery = async (query) => {
  const connection = await connectToDatabricks();
  const session = await connection.openSession();
  try {
    const queryOperation = await session.executeStatement(query, {
      runAsync: true,
    });
    const result = await queryOperation.fetchAll();
    await queryOperation.close();
    return result;
  } finally {
    await session.close();  // Cerrar sesion pero no la conexión
  }
};

app.post('/api/query-databricks', async (req, res) => {
  const { query } = req.body;

  try {
    const results = await executeQuery(query);
    console.log(results);
    res.json(results);
  } catch (error) {
    console.error('Error executing query: ', error);
    res.status(500).send('Error executing query');
  }
});

app.listen(port, () => {
  console.log(`Server running on port ${port}`);
});