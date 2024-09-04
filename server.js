import { DBSQLClient } from '@databricks/sql';
import  express from 'express';
import dotenv from 'dotenv';

const app = express();
dotenv.config();
const port = process.env.PORT || 3010;

app.use(express.json());

const token = process.env.TOKEN;
const server_hostname = process.env.SERVER_HOSTNAME;
const http_path = process.env.HTTP_PATH;

const client = new DBSQLClient();

const connectToDatabricks = async (query) => {
  try {
    const clientConnection = await client.connect({
      token: token,
      host: server_hostname,
      path: http_path,
    });

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