import { DBSQLClient } from '@databricks/sql';
import  express from 'express';
import dotenv from 'dotenv';
import cors from 'cors';
import compression from 'compression';

const app = express();
dotenv.config();
const port = process.env.PORT || 3010;

app.use(express.json());
app.use(cors());
app.use(compression());

const token = "dapie6b8d05bc18929b4e120930c1152ed71-3";
const server_hostname = "adb-4821506742419671.11.azuredatabricks.net";
const http_path = "/sql/1.0/warehouses/a45ce58754c146b6";

const client = new DBSQLClient();
let clientConnection; 

const connectToDatabricks = async (query) => {
  try {
    console.log('TOKEN:', token);
    console.log('SERVER_HOSTNAME:', server_hostname);
    console.log('HTTP_PATH:', http_path);
    if (!clientConnection) {
      clientConnection = await client.connect({
        token: token,
        host: server_hostname,
        path: http_path,
      });
    }

    const session = await clientConnection.openSession();
    const queryOperation = await session.executeStatement(query, {
      runAsync: true,
    });

    const result = await queryOperation.fetchAll();
    await queryOperation.close();
    await session.close();
    await clientConnection.close();

    console.table(result);

    return result;
  } catch (error) {
    console.error("Error connecting to Databricks:", error);
    throw error;
  }
};

app.post('/api/query-databricks', async (req, res) => {
  const { query } = req.body;

  try {
    const results = await connectToDatabricks(query);
    res.json(results);
  } catch (error) {
    console.error('Error executing query: ', error);
    res.status(500).send('Error executing query');
  }
});

app.listen(port, () => {
  console.log(`Server running on port ${port}`);
});