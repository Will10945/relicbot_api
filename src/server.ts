// import mysql from 'mysql2';
import express from 'express';
import helmet from 'helmet';
import morgan from 'morgan';
import config from 'config';

const startupDebugger = require('debug')('app:startup');

import logger from './middleware/logger';
import members from './routes/members';
import squads from './routes/squads';
import home from './routes/home';

// const pool = mysql.createPool({
//     host: dbConfig.HOST,
//     user: dbConfig.USER,
//     password: dbConfig.PASSWORD,
//     database: dbConfig.DB
// });

const app = express();

app.set('view engine', 'pug');

app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(express.static('public'));
app.use(helmet());
app.use('/api/members', members);
app.use('/api/squads', squads);
app.use('/', home);

// Configuration
// console.log('Application Name: ' + config.get('name'));

if (app.get('env') === 'development'){
    app.use(morgan('tiny'));
    startupDebugger('Morgan enabled...');
}

app.use(logger);

// app.use(bodyParser.json());

const port = process.env.PORT || 3000;
app.listen(port, () => console.log(`Server running on port ${port}`));
