import React, { FC } from "react";
import { Button, CardBody, CardHeader, Table, CardFooter, Badge } from "reactstrap";
import styled from "styled-components/macro";
import { faArrowRight } from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";

import { Alert, Rule } from "../interfaces";
import { CenteredContainer } from "./CenteredContainer";
import { ScrollingCol} from "./App";
import { Payment, Payee, Details, Beneficiary, paymentTypeMap } from "./Transactions";
import { Line } from "app/utils/useLines";

const AlertTable = styled(Table)`
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

export const Alerts: FC<Props> = props => {

  const tooManyAlerts = props.alerts.length > 40;

  const handleScroll = () => {
    props.lines.forEach(line => line.line.position());
  };

  return (
    <ScrollingCol xs={{ size: 5, offset: 1 }} onScroll={handleScroll}>
      {props.alerts.map((alert, idx) => {
        console.log(alert)
        return (
          <CenteredContainer
            key={idx}
            className="w-100"
            ref={alert.ref}
            tooManyItems={tooManyAlerts}
            style={{ borderColor: "#ffc107", borderWidth: 2 }}
          >
            <CardHeader>
              <Button size="sm" color="primary" onClick={props.clearAlert(idx)} className="mr-3">
                            Clear Event
              </Button>
              Event at {new Date(alert.timestamp).toLocaleString()}
            </CardHeader>
            <CardBody className="p-0">
              <AlertTable size="sm" bordered={true}>
                <tbody>
                  <tr>
                    <th style={{ width: "15%" }}>Response</th>
                    <th style={{ width: "85%" }}>
                    {alert.response.map(text =>{
                       return <td><tr>{"col" + alert.response.indexOf(text).toString()}</tr><tr>{text}</tr></td>;
                       })}

                    </th>
                  </tr>
                </tbody>
              </AlertTable>
            </CardBody>
          </CenteredContainer>
        );
      })}
    </ScrollingCol>
  );
};

interface Props {
  alerts: Alert[];
  clearAlert: any;
  lines: Line[];
  // handleScroll: () => void;
}
