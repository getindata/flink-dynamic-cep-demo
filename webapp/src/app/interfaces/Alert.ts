import { Transaction } from "./Transaction";
import { RefObject } from "react";

export interface Alert {
  alertId: string;
  isAdded: boolean;
  timestamp: number;
  response: string[];
  sql: string;
  ref: RefObject<HTMLDivElement>;
  timeout: number
}
