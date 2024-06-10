import type { RowDataPacket } from 'mysql2';

export default interface IPrimePartRow extends RowDataPacket {
    PrimeSet: string;
    Part: string;
    Price: number;
    Ducats: number;
    Required: number;
}