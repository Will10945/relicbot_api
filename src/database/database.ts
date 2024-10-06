import mysql, { ResultSetHeader } from 'mysql2/promise';
import dotenv from 'dotenv';
import IMemberRow from '../entities/db.member';
import ISquadRow, { ISquadPostRow, ISquadUserRow, ISquadRelicRow, ISquadRefinementRow } from '../entities/db.squads';
import IRelicRow from '../entities/db.relics';
import IPrimeSetRow from '../entities/db.primeSets';
import IPrimePartRow from '../entities/db.primeParts';
import IRefinementRow from '../entities/db.refinement';
import TABLES from '../entities/constants';

dotenv.config();

const dbDebugger = require('debug')('app:db');

const pool = mysql.createPool({
    host: process.env.MYSQL_HOST,
    user: process.env.MYSQL_USER,
    password: process.env.MYSQL_PASSWORD,
    database: process.env.MYSQL_DATABASE
});
dbDebugger('Connected to the database...');


export async function SelectQuery<T>(queryString: string, params?: (number | string)[]): Promise<T[]> {
    const [results] = await pool.execute(queryString, params);
    dbDebugger(`SelectQuery: ${queryString} using the params: ${params}`);
    return results as T[];
}

export async function ModifyQuery(queryString: string, params?: (number | string)[]): Promise<ResultSetHeader> {
    const [results] = await pool.execute(queryString, params);
    dbDebugger(`ModifyQuery: ${queryString} using the params: ${params}`);
    return results as ResultSetHeader;
}

export async function getAllMembers() {
    return await SelectQuery<IMemberRow>(`SELECT * FROM ${TABLES.MEMBERS}`);
}

export async function getMemberById(id: number) {
    return await SelectQuery<IMemberRow>(`SELECT * FROM ${TABLES.MEMBERS} WHERE MemberID = ?`, [id]);
}

export async function getMemberByName(name: string|number) {
    return await SelectQuery<IMemberRow>(`SELECT * FROM ${TABLES.MEMBERS} WHERE MemberName = ?`, [name]);
}

export async function getAllSquadsWithMessages() {
    var squads: ISquadRow[] = [];
    var offset: number = 0;
    const limit: number = 100000;
    while (true){
        const _results = await SelectQuery<ISquadRow>(
            `SELECT * FROM ${TABLES.SQUADS} 
            LIMIT ${limit} OFFSET ${offset};`
        );
        squads = squads.concat(_results);
        if (_results.length == 0) break;
        offset += limit;
    }
    const squadUsers = await getAllSquadUsers();
    const squadRelics = await getAllSquadRelics();
    const squadRefinements = await getAllSquadRefinements();
    const squadPosts = await getAllSquadPosts();
    return { squads, squadUsers, squadRelics, squadRefinements, squadPosts };
}

export async function getAllSquads() {
    var squads: ISquadRow[] = [];
    var offset: number = 0;
    const limit: number = 100000;
    while (true){
        const _results = await SelectQuery<ISquadRow>(
            `SELECT * FROM ${TABLES.SQUADS} 
            LIMIT ${limit} OFFSET ${offset};`
        );
        squads = squads.concat(_results);
        if (_results.length == 0) break;
        offset += limit;
    }
    const squadUsers = await getAllSquadUsers();
    const squadRelics = await getAllSquadRelics();
    const squadRefinements = await getAllSquadRefinements();
    const squadPosts: ISquadPostRow[] = [];
    return { squads, squadUsers, squadRelics, squadRefinements, squadPosts };
}

export async function getActiveSquads() {
    var squads: ISquadRow[] = [];
    var offset: number = 0;
    const limit: number = 100000;
    while (true){
        const _results = await SelectQuery<ISquadRow>(
            `SELECT * FROM ${TABLES.SQUADS} WHERE Active = 1 
            LIMIT ${limit} OFFSET ${offset};`
        );
        squads = squads.concat(_results);
        if (_results.length == 0) break;
        offset += limit;
    }
    const squadUsers = await getAllSquadUsers();
    const squadRelics = await getAllSquadRelics();
    const squadRefinements = await getAllSquadRefinements();
    const squadPosts: ISquadPostRow[] = [];
    return { squads, squadUsers, squadRelics, squadRefinements, squadPosts };
}

export async function getSquadById(id: string) {
    const squad = await SelectQuery<ISquadRow>(`SELECT * FROM ${TABLES.SQUADS} WHERE SquadID = ?;`, [id]);
    const squadUsers = await SelectQuery<ISquadUserRow>(`SELECT * FROM ${TABLES.SQUADUSERS} WHERE SquadID = ?;`, [id]);
    const squadRelics = await SelectQuery<ISquadRelicRow>(`SELECT * FROM ${TABLES.SQUADRELICS} WHERE SquadID = ?;`, [id]);
    const squadRefinements = await SelectQuery<ISquadRefinementRow>(`SELECT * FROM ${TABLES.SQUADREFINEMENT} WHERE SquadID = ?;`, [id]);
    const squadPosts = await SelectQuery<ISquadPostRow>(`SELECT * FROM ${TABLES.SQUADPOSTS} WHERE SquadID = ?;`, [id]);

    return { squad, squadUsers, squadRelics, squadRefinements, squadPosts };
}

export async function getAllSquadUsers() {
    return await SelectQuery<ISquadUserRow>(
        `SELECT * FROM ${TABLES.SQUADUSERS};`
    );
}

export async function getAllSquadRelics() {
    return await SelectQuery<ISquadRelicRow>(
        `SELECT * FROM ${TABLES.SQUADRELICS};`
    );
}

export async function getAllSquadRefinements() {
    return await SelectQuery<ISquadRefinementRow>(
        `SELECT * FROM ${TABLES.SQUADREFINEMENT};`
    );
}

export async function getAllSquadPosts() {
    return await SelectQuery<ISquadPostRow>(
        `SELECT * FROM ${TABLES.SQUADPOSTS};`
    );
}

export async function getAllRelics() {
    return await SelectQuery<IRelicRow>(
        `SELECT * FROM ${TABLES.RELICS}`
    );
}

export async function getRelic(id: number) {
    return await SelectQuery<IRelicRow>(
        `SELECT * FROM ${TABLES.RELICS} WHERE ID = ?`, [id]
    )
}

export async function getAllPrimeSets() {
    return await SelectQuery<IPrimeSetRow>(
        `SELECT * FROM ${TABLES.PRIMESETS}`
    );
}

export async function getAllPrimeParts() {
    return await SelectQuery<IPrimePartRow>(
        `SELECT * FROM ${TABLES.PRIMEPARTS}`
    );
}

export async function getAllRefinements() {
    return await SelectQuery<IRefinementRow>(
        `SELECT * FROM ${TABLES.REFINEMENT}`
    );
}
