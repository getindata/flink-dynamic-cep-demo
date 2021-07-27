import {
  faArrowUp,
  faCalculator,
  faClock,
  faFont,
  faInfoCircle,
  faLaptopCode,
  faLayerGroup,
} from "@fortawesome/free-solid-svg-icons";
import Axios from "axios";
import getFormData from "get-form-data";
import { isArray, pick } from "lodash/fp";
import React, { createRef, FC, FormEvent, useState, MouseEvent } from "react";
import CreatableSelect from "react-select/creatable";
import { Alert, Button, Input, Modal, ModalBody, ModalFooter, ModalHeader } from "reactstrap";
import { Rule, RulePayload } from "../interfaces/";
import { FieldGroup } from "./FieldGroup";

const headers = { "Content-Type": "application/json" };

const pickFields = pick([
  "sql",
]);

type ResponseError = {
  error: string;
  message: string;
} | null;

const sampleRules: {
  [n: number]: string;
} = {

};

const keywords = ["beneficiaryId", "payeeId", "paymentAmount", "paymentType"];
const aggregateKeywords = ["paymentAmount", "COUNT_FLINK", "COUNT_WITH_RESET_FLINK"];

const MySelect = React.memo(CreatableSelect);

export const AddRuleModal: FC<Props> = props => {
  const [error, setError] = useState<ResponseError>(null);

  const handleClosed = () => {
    setError(null);
    props.onClosed();
  };

  const handleSubmit = (e: FormEvent) => {
    e.preventDefault();
    const data = pickFields(getFormData(e.target)) as RulePayload;
    const content = data.sql;
    console.log("Submitting sql: " + content)

    const body = JSON.stringify({ content });

    setError(null);
    Axios.post<Rule>("/api/sqls", body, { headers })
      .then(response => {
      console.log("POST");
      console.log(body);
      console.log("RESPONSE");
      console.log(response);
      props.setRules(rules => {
          const newRule = { ...response.data, ref: createRef<HTMLDivElement>() };
          console.log("NEW RULE");
          console.log(newRule);
          return [...rules, newRule];
      })
      })
      .then(props.onClosed)
      .catch(setError);
  };

  const postSampleRule = (ruleId: number) => (e: MouseEvent) => {
    const rulePayload = JSON.stringify(sampleRules[ruleId]);
    const body = JSON.stringify({ rulePayload });

    Axios.post<Rule>("/api/sqls", body, { headers })
      .then(response => props.setRules(rules => [...rules, { ...response.data, ref: createRef<HTMLDivElement>() }]))
      .then(props.onClosed)
      .catch(setError);
  };

  return (
    <Modal
      isOpen={props.isOpen}
      onClosed={handleClosed}
      toggle={props.toggle}
      backdropTransition={{ timeout: 75 }}
      modalTransition={{ timeout: 150 }}
      size="lg"
    >
      <form onSubmit={handleSubmit}>
        <ModalHeader toggle={props.toggle}>Add a new Rule</ModalHeader>
        <ModalBody>
          {error && <Alert color="danger">{error.error + ": " + error.message}</Alert>}
          <FieldGroup label="SQL" icon={faInfoCircle}>
            <Input type="textarea" name="sql" bsSize="sm" />
          </FieldGroup>
        </ModalBody>
        <ModalFooter className="justify-content-between">
          <div>
            <Button color="secondary" onClick={postSampleRule(1)} size="sm" className="mr-2">
              Sample Rule 1
            </Button>
            <Button color="secondary" onClick={postSampleRule(2)} size="sm" className="mr-2">
              Sample Rule 2
            </Button>
            <Button color="secondary" onClick={postSampleRule(3)} size="sm" className="mr-2">
              Sample Rule 3
            </Button>
          </div>
          <div>
            <Button color="secondary" onClick={handleClosed} size="sm" className="mr-2">
              Cancel
            </Button>
            <Button type="submit" color="primary" size="sm">
              Submit
            </Button>
          </div>
        </ModalFooter>
      </form>
    </Modal>
  );
};

interface Props {
  toggle: () => void;
  isOpen: boolean;
  onClosed: () => void;
  setRules: (fn: (rules: Rule[]) => Rule[]) => void;
}
