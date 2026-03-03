import express from 'express';
import helmet from 'helmet';
import morgan from 'morgan';
import path from 'path';
import http from 'http';
import cors from 'cors';
import compression from 'compression';
import cookieParser from 'cookie-parser';

const startupDebugger = require('debug')('app:startup');

import logger from './middleware/logger';
import { apiKeyAuth } from './middleware/apiKeyAuth';
import { requireScope } from './middleware/requireScope';
import { sessionAuth } from './middleware/sessionAuth';
import members from './routes/members';
import squads from './routes/squads';
import relics from './routes/relics';
import hosts from './routes/hosts';
import primeSets from './routes/primeSets';
import primeParts from './routes/primeParts';
import refinements from './routes/refinements';
import home from './routes/home';
import auth from './routes/auth';
import { ServerSocket } from './socket';

const app = express();

/** Server Handling */
const httpServer = http.createServer(app);

/** Start Socket */
new ServerSocket(httpServer);

app.set('view engine', 'pug');
app.set('views', path.join(__dirname, '../public/views'));

/** CORS: allow any origin; handle OPTIONS (204) and set headers on all responses. credentials for session cookies. */
app.use(
  cors({
    origin: true,
    methods: ['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization', 'X-API-Key'],
    credentials: true,
    optionsSuccessStatus: 204,
  })
);

app.use(compression());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(cookieParser());
app.use(express.static('public'));
app.use(helmet());
app.use(sessionAuth);

app.use('/api', apiKeyAuth, requireScope);
app.use('/api/members', members);
app.use('/api/squads', squads);
app.use('/api/relics', relics);
app.use('/api/hosts', hosts);
app.use('/api/primesets', primeSets);
app.use('/api/primeparts', primeParts);
app.use('/api/refinements', refinements);

app.use('/auth', auth);
app.use('/', home);

// Configuration
if (app.get('env') === 'development'){
    app.use(morgan('tiny'));
    startupDebugger('Morgan enabled...');
}

app.use(logger);

const port = process.env.PORT || 3000;
httpServer.listen(port, () => console.log(`Server running on port ${port}`));
