use super::*;

pub(super) struct JavaScriptDomain {
    pub(super) identity: u128,
    pub(super) validate: Function,
    pub(super) observe: Function,
    pub(super) corresponds: Function,
    pub(super) maximum_bytes: usize,
    pub(super) restore: Option<Function>,
}

impl Domain for JavaScriptDomain {
    type Key = String;
    type Basis = String;
    type Operation = String;
    type Assumption = JsValue;
    type Value = JsValue;
    type Evidence = JsValue;

    fn identity(&self) -> u128 {
        self.identity
    }

    fn restore(
        &self,
        fact: &Fact<String, String, JsValue>,
        branch: &super::super::ClientHypothesis<Self>,
        work: usize,
    ) -> Result<(usize, usize), Error> {
        let callback = self.restore.as_ref().ok_or(Error::Unsupported)?;
        let result = callback
            .call3(
                &JsValue::UNDEFINED,
                &fact_object(fact)?,
                &hypothesis_object(branch)?,
                &encode(&work)?,
            )
            .map_err(|_| Error::Unsupported)?;
        Ok((
            decode(field(&result, "bytes")?)?,
            decode(field(&result, "work")?)?,
        ))
    }

    fn validate(
        &self,
        fact: &Fact<String, String, JsValue>,
        value: &JsValue,
        assumption: &JsValue,
        work: usize,
    ) -> Result<(usize, usize), Error> {
        let args = js_sys::Array::new();
        args.push(&fact_object(fact)?);
        args.push(value);
        args.push(assumption);
        args.push(&encode(&work)?);
        let result = self
            .validate
            .apply(&JsValue::UNDEFINED, &args)
            .map_err(|_| Error::Unsupported)?;
        Ok((
            decode(field(&result, "bytes")?)?,
            decode(field(&result, "work")?)?,
        ))
    }

    fn observe(
        &self,
        evidence: &JsValue,
        current: Option<&Fact<String, String, JsValue>>,
        work: usize,
    ) -> Result<Observation<String, String, String, JsValue>, Error> {
        let current = current
            .map(fact_object)
            .transpose()?
            .unwrap_or(JsValue::NULL);
        let result = self
            .observe
            .call3(&JsValue::UNDEFINED, evidence, &current, &encode(&work)?)
            .map_err(|_| Error::Unsupported)?;
        let operation = field(&result, "operation")?;
        let operation = if operation.is_null() || operation.is_undefined() {
            None
        } else {
            let operation = array(&operation, 2)?;
            if operation.length() != 2 {
                return Err(Error::Conflict);
            }
            Some((
                text(&operation.get(0), self.maximum_bytes)?,
                decode(operation.get(1))?,
            ))
        };
        let value = field(&result, "fact")?;
        let fact = if value.is_null() || value.is_undefined() {
            None
        } else {
            Some(Fact {
                key: text(&field(&value, "key")?, self.maximum_bytes)?,
                basis: Arc::new(text(&field(&value, "basis")?, self.maximum_bytes)?),
                value: Arc::new(field(&value, "value")?),
                bytes: decode(field(&value, "bytes")?)?,
            })
        };
        Ok(Observation {
            fact,
            operation,
            work: decode(field(&result, "work")?)?,
        })
    }

    fn corresponds(
        &self,
        predicted: &JsValue,
        canonical: &JsValue,
        work: usize,
    ) -> Result<(Correspondence, usize), Error> {
        let result = self
            .corresponds
            .call3(&JsValue::UNDEFINED, predicted, canonical, &encode(&work)?)
            .map_err(|_| Error::Unsupported)?;
        let matches: bool = decode(field(&result, "matches")?)?;
        Ok((
            if matches {
                Correspondence::Match
            } else {
                Correspondence::Different
            },
            decode(field(&result, "work")?)?,
        ))
    }
}
