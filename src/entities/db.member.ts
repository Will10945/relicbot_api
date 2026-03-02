
import type { RowDataPacket } from 'mysql2';

export default interface IMemberRow extends RowDataPacket {
    MemberID?: number;
    DiscordID?: number | string; /* string preferred to avoid JS number precision loss for large IDs */
    MemberName?: string;
    AllowMerging?: number;
    HostLimit?: number;
    CycleDefault?: number;
    Muted?: number;
    Badge?: number;
    SquadChatTime?: number;
    Admin?: number;
}