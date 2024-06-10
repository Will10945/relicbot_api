import type { RowDataPacket } from 'mysql2';

export default interface IPrimeSetRow extends RowDataPacket {
    PrimeSet: string;
    Price: number;
    Ducats: number;
    PrimeAccess: string;
    Category: string;
    Vaulted: number;
}