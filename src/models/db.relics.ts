
import type { RowDataPacket } from 'mysql2';

export default interface IRelicRow extends RowDataPacket {
    ID: number;
    Era: string;
    Name: string;
    Relic: string;
    Vaulted: number;
    Common1?: string;
    Common2?: string;
    Common3?: string;
    Uncommon1?: string;
    Uncommon2?: string;
    Rare?: string;
}