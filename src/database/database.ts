import mysql, { ResultSetHeader } from 'mysql2/promise';
import dotenv from 'dotenv';
import IMemberRow from '../models/db.member';
import ISquadRow, { ISquadPostRow, ISquadUserRow, ISquadRelicRow, ISquadRefinementRow } from '../models/db.squads';
import IRelicRow from '../models/db.relics';
import IPrimeSetRow from '../models/db.primeSets';
import IPrimePartRow from '../models/db.primeParts';

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
    return await SelectQuery<IMemberRow>('SELECT * FROM Members');
}

export async function getMemberById(id: number) {
    return await SelectQuery<IMemberRow>(`SELECT * FROM Members WHERE MemberID = ?`, [id]);
}

export async function getMemberByName(name: string|number) {
    return await SelectQuery<IMemberRow>(`SELECT * FROM Members WHERE MemberName = ?`, [name]);
}

export async function getAllSquadsWithMessages() {
    var squads: ISquadRow[] = [];
    var offset: number = 0;
    const limit: number = 100000;
    while (true){
        const _results = await SelectQuery<ISquadRow>(
            `SELECT * FROM Squads 
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
            `SELECT * FROM Squads 
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
    const squad = await SelectQuery<ISquadRow>(`SELECT * FROM Squads WHERE SquadID = ?;`, [id]);
    const squadUsers = await SelectQuery<ISquadUserRow>(`SELECT * FROM SquadUsers WHERE SquadID = ?;`, [id]);
    const squadRelics = await SelectQuery<ISquadRelicRow>(`SELECT * FROM SquadRelics WHERE SquadID = ?;`, [id]);
    const squadRefinements = await SelectQuery<ISquadRefinementRow>(`SELECT * FROM SquadRefinement WHERE SquadID = ?;`, [id]);
    const squadPosts = await SelectQuery<ISquadPostRow>(`SELECT * FROM SquadPosts WHERE SquadID = ?;`, [id]);

    return { squad, squadUsers, squadRelics, squadRefinements, squadPosts };
}

export async function getAllSquadUsers() {
    return await SelectQuery<ISquadUserRow>(
        `SELECT * FROM SquadUsers;`
    );
}

export async function getAllSquadRelics() {
    return await SelectQuery<ISquadRelicRow>(
        `SELECT * FROM SquadRelics;`
    );
}

export async function getAllSquadRefinements() {
    return await SelectQuery<ISquadRefinementRow>(
        `SELECT * FROM SquadRefinement;`
    );
}

export async function getAllSquadPosts() {
    return await SelectQuery<ISquadPostRow>(
        `SELECT * FROM SquadPosts;`
    );
}

export async function getAllRelics() {
    return await SelectQuery<IRelicRow>(
        `SELECT * FROM Relics`
    );
}

export async function getAllPrimeSets() {
    return await SelectQuery<IPrimeSetRow>(
        `SELECT * FROM PrimeSets`
    );
}

export async function getAllPrimeParts() {
    return await SelectQuery<IPrimePartRow>(
        `SELECT * FROM PrimeParts`
    );
}
