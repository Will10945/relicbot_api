import type { RowDataPacket } from 'mysql2';

export default interface IRelicDropDataRow extends RowDataPacket {
    RelicID: number;
    PartName: string;
    Rarity: string;
    Ducats: number;
    Price: number;
    Chances: string | null;
}
