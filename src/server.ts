import express from 'express';
import helmet from 'helmet';
import morgan from 'morgan';
import path from 'path';
import http from 'http';

const startupDebugger = require('debug')('app:startup');

import logger from './middleware/logger';
import members from './routes/members';
import squads from './routes/squads';
import relics from './routes/relics';
import primeSets from './routes/primeSets';
import primeParts from './routes/primeParts';
import refinements from './routes/refinements';
import home from './routes/home';
import { ServerSocket } from './socket';

const allowCrossDomain = function(req: any, res: any, next: any) {
    res.header('Access-Control-Allow-Origin', "*");
    res.header('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE');
    res.header('Access-Control-Allow-Headers', 'Content-Type');
    next();
}

const app = express();

app.use(allowCrossDomain);

/** Server Handling */
const httpServer = http.createServer(app);

/** Start Socket */
new ServerSocket(httpServer);

app.set('view engine', 'pug');
app.set('views', path.join(__dirname, '../public/views'));

app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(express.static('public'));
app.use(helmet());
app.use('/api/members', members);
app.use('/api/squads', squads);
app.use('/api/relics', relics);
app.use('/api/primesets', primeSets);
app.use('/api/primeparts', primeParts);
app.use('/api/refinements', refinements);
app.use('/', home);

// Configuration
if (app.get('env') === 'development'){
    app.use(morgan('tiny'));
    startupDebugger('Morgan enabled...');
}

app.use(logger);

const port = process.env.PORT || 3000;
app.listen(port, () => console.log(`Server running on port ${port}`));
