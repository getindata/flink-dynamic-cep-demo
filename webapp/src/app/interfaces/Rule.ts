import { RefObject } from "react";

export interface Rule {
  id: number;
  content: string;
  ref: RefObject<HTMLDivElement>;
}

export interface RulePayload {
  sql: string;
}