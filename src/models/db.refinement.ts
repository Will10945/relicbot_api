
import type { RowDataPacket } from 'mysql2';

export default interface IRefinementRow extends RowDataPacket {
    ID: number;
    Name: string;
}