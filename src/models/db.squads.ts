
import type { RowDataPacket } from 'mysql2';

export default interface ISquadRow extends RowDataPacket {
    SquadID: string;
    Style?: string;
    Era?: string;
    CycleRequirement?: number;
    Host?: number;
    CurrentCount?: number;
    Filled?: number;
    UserMsg?: string;
    CreatedAt?: number;
    Active?: number;
    OriginatingServer?: number;
    Rehost?: number;
    ClosedAt?: number;
}

export interface ISquadUserRow extends RowDataPacket {
    SquadID: string;
    MemberID: number;
    ServerID: number;
    AnonymousUsers: number;
}

export interface ISquadRelicRow extends RowDataPacket {
    SquadID: string;
    RelicID: number;
    Offcycle: number;
}

export interface ISquadRefinementRow extends RowDataPacket {
    SquadID: string;
    RefinementID: number;
    Offcycle: number;
}

export interface ISquadPostRow extends RowDataPacket {
    SquadID: string;
    MessageID: number;
    ChannelID: number;
}

export interface Squad{
    SquadID: string;
    Style?: string;
    Era?: string;
    CycleRequirement?: number;
    Host?: number;
    CurrentCount?: number;
    Filled?: number;
    UserMsg?: string;
    CreatedAt?: number;
    Active?: number;
    OriginatingServer?: number;
    Rehost?: number;
    ClosedAt?: number;
    MemberIDs: {
        [MemberID: number]: {
            ServerID?: number;
            AnonymousUsers?: number;
        }
    };
    RelicIDs: {
        [RelicID: number]: {
            Offcycle?: number;
        }
    };
    RefinementIDs: {
        Oncycle: number[];
        Offcycle: number[];
    }
    MessageIDs?: {
        [MessageID: number]: {
            ChannelID?: number;
        }
    };
}