use super::*;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    // Catalog tools retain every declared argument; native-name translation
    // never replaces the schema advertised to Pi.
    #[test]
    fn pi_catalog_shell_arguments_round_trip(
        command in "[a-z ]{1,32}", cwd in "[a-z]{1,12}(/[a-z]{1,12})?", timeout in 1u32..300,
    ) {
        let input = json!({"command":command,"cwd":cwd,"timeout":timeout});
        let request = super::super::request(ExternalHost::Pi,"tool_call",json!({
            "sessionId":"options","toolCallId":"bash","toolName":"bash","input":input,
        }))?;
        let call = request.tool_call(&crate::tools::defaults()?)?;
        prop_assert_eq!(&call.arguments,&input);
        let decoded: crate::tools::BashInput = serde_json::from_value(call.arguments)?;
        prop_assert_eq!(decoded.command,command);
        prop_assert_eq!(decoded.cwd,Some(cwd));
        prop_assert_eq!(decoded.timeout,Some(timeout));
    }
}
