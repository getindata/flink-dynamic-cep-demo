import { library } from "@fortawesome/fontawesome-svg-core";
import {
  faArrowUp,
  faCalculator,
  faClock,
  faFont,
  faInfoCircle,
  faLaptopCode,
  faLayerGroup,
  IconDefinition,
} from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import Axios from "axios";
import { isArray } from "lodash/fp";
import React, { FC } from "react";

import { Badge, Button, CardBody, CardFooter, CardHeader, Table } from "reactstrap";
import styled from "styled-components/macro";
import { Alert, Rule } from "../interfaces";
import { CenteredContainer } from "./CenteredContainer";
import { ScrollingCol } from "./App";
import { Line } from "app/utils/useLines";

library.add(faInfoCircle);

const badgeColorMap: {
  [s: string]: string;
} = {
  ACTIVE: "success",
  DELETE: "danger",
  PAUSE: "warning",
};

const iconMap: {
  [s: string]: IconDefinition;
} = {
  aggregateFieldName: faFont,
  aggregatorFunctionType: faCalculator,
  groupingKeyNames: faLayerGroup,
  limit: faArrowUp,
  limitOperatorType: faLaptopCode,
  windowMinutes: faClock,
};

const seperator: {
  [s: string]: string;
} = {
  EQUAL: "to",
  GREATER: "than",
  GREATER_EQUAL: "than",
  LESS: "than",
  LESS_EQUAL: "than",
  NOT_EQUAL: "to",
};

const RuleTitle = styled.div`
  display: flex;
  align-items: center;
`;

const RuleTable = styled(Table)`
  && {
    width: calc(100% + 1px);
    border: 0;
    margin: 0;

    td {
      vertical-align: middle !important;

      &:first-child {
        border-left: 0;
      }

      &:last-child {
        border-right: 0;
      }
    }

    tr:first-child {
      td {
        border-top: 0;
      }
    }
  }
`;


// const omitFields = omit(["ruleId", "ruleState", "unique"]);

const hasAlert = (alerts: Alert[], rule: Rule) => alerts.some(alert => alert.sql === rule.content);

export const Rules: FC<Props> = props => {
  const handleDelete = (id: number) => () => {
    Axios.delete(`/api/sqls/${id}`).then(props.clearRule(id));
  };

  const handleScroll = () => {
    props.ruleLines.forEach(line => line.line.position());
    props.alertLines.forEach(line => line.line.position());
  };

  const tooManyRules = props.rules.length > 3;

  return (
    <ScrollingCol xs={{ size: 3, offset: 1 }} onScroll={handleScroll}>
      {props.rules.map(rule => {
        if (!rule.content) {
          return null;
        }

        return (
          <CenteredContainer
            ref={rule.ref}
            key={rule.id}
            tooManyItems={tooManyRules}
            style={{
              borderColor: hasAlert(props.alerts, rule) ? "#dc3545" : undefined,
              borderWidth: hasAlert(props.alerts, rule) ? 2 : 1,
            }}
          >
            <CardHeader className="d-flex justify-content-between align-items-center" style={{ padding: "0.3rem" }}>
              <RuleTitle>
                <FontAwesomeIcon icon={faInfoCircle} fixedWidth={true} className="mr-2" />
                Rule #{rule.id}{" "}
              </RuleTitle>
              <Button size="sm" color="danger" outline={true} onClick={handleDelete(rule.id)}>
                Delete
              </Button>
            </CardHeader>
            <CardFooter style={{ padding: "0.3rem", whiteSpace: "pre-wrap" }}>
              <em>{rule.content}</em>
            </CardFooter>
          </CenteredContainer>
        );
      })}
    </ScrollingCol>
  );
};

interface Props {
  alerts: Alert[];
  rules: Rule[];
  clearRule: (id: number) => () => void;
  ruleLines: Line[];
  alertLines: Line[];
}
