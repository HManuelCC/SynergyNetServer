export interface ClientHardwareResourcesStatistics {
  CPUUsage: number;
  MemoryUsage: number;
  DiskUsage: number;
  DiskBusy: number;
}

export interface EventsSubscribed {
  events: string[];
}

export interface ClientInformation {
  client_name: string;
  latency: number;
  resources: ClientHardwareResourcesStatistics;
  events: EventsSubscribed;
}

export interface Event {
  event: string;
  destination?: string;
  data: any;
  origen?: string;
  pid?: number;
}

export interface State {
  status: boolean;
  state: string; // Message field in Go is tagged as `state`
  error: string;
  data: any;
  destination?: string;
  origen?: string;
  pid?: number;
}

export interface MessageState {
  status: boolean;
  server_pid: number;
  state: string;
  error: string;
  process_status: number; // 0=pendiente,1=en proceso,2=finalizado
}

export type ResponseCallback = (response: State) => void;

export interface Process {
  pid: number;
  ttl: number;
  attempts: number;
  created: Date;
  updated: Date;
  data: (value: any) => void; // simplified callback storage
  onTimeout?: () => void;
}
