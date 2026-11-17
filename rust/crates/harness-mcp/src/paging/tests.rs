use super::*;
use futures::{StreamExt, TryStreamExt};
use serde_json::json;
use std::{cell::RefCell, rc::Rc, sync::Arc};

fn page(name: &str, cursor: Option<String>) -> rmcp::model::ListToolsResult {
    let mut tool = rmcp::model::Tool::default();
    tool.name = name.to_owned().into();
    tool.input_schema = Arc::new(serde_json::from_value(json!({"type":"object"})).unwrap());
    let mut page = rmcp::model::ListToolsResult::default();
    page.tools = vec![tool];
    page.next_cursor = cursor;
    page
}

#[tokio::test]
async fn pages_are_demand_driven_move_schemas_and_have_no_total_bound() -> Result<()> {
    let requests = Rc::new(RefCell::new(Vec::new()));
    let observed = Rc::clone(&requests);
    let first = page("first", Some("1".into()));
    let pointer = &first.tools[0].input_schema["type"] as *const serde_json::Value;
    let mut first = Some(first);
    let pages = tool_pages(
        move |cursor| {
            observed.borrow_mut().push(cursor.clone());
            let page = first.take().unwrap_or_else(|| {
                let index: usize = cursor.unwrap().parse().unwrap();
                page(
                    &format!("tool{index}"),
                    (index < 256).then(|| (index + 1).to_string()),
                )
            });
            std::future::ready(Ok(page))
        },
        McpDiscoveryLimits::default(),
        |_| true,
    )?;
    assert!(requests.borrow().is_empty());
    futures::pin_mut!(pages);
    let first = pages.try_next().await?.unwrap();
    assert_eq!(&first[0].input_schema["type"] as *const _, pointer);
    assert_eq!(*requests.borrow(), [None]);
    let mut total = first.len();
    while let Some(page) = pages.try_next().await? {
        total += page.len();
    }
    assert_eq!(total, 257);
    assert_eq!(requests.borrow().len(), 257);
    assert!(pages.next().await.is_none());
    assert_eq!(requests.borrow().len(), 257);
    Ok(())
}

#[tokio::test]
async fn empty_pages_continue_and_invalid_cursor_cycles_terminate() -> Result<()> {
    for cursors in [vec![Some("")], vec![Some("a"), Some("b"), Some("a")]] {
        let expected = cursors.len();
        let mut cursors = cursors.into_iter();
        let pages = tool_pages(
            move |_| {
                let mut page = rmcp::model::ListToolsResult::default();
                page.next_cursor = cursors
                    .next()
                    .expect("no request after cursor error")
                    .map(str::to_owned);
                std::future::ready(Ok(page))
            },
            McpDiscoveryLimits::default(),
            |_| true,
        )?;
        futures::pin_mut!(pages);
        for _ in 1..expected {
            assert!(pages.try_next().await?.unwrap().is_empty());
        }
        assert!(matches!(pages.try_next().await, Err(Error::Invalid(_))));
        assert!(pages.next().await.is_none());
    }
    let pages = tool_pages(
        |_| std::future::ready(Ok(rmcp::model::ListToolsResult::default())),
        McpDiscoveryLimits::default(),
        |_| true,
    )?;
    futures::pin_mut!(pages);
    assert!(pages.try_next().await?.unwrap().is_empty());
    assert!(pages.next().await.is_none());
    Ok(())
}

#[tokio::test]
async fn dropping_a_page_request_retires_it_without_prefetch() {
    let (request, requested) = tokio::sync::oneshot::channel();
    let (release, wait) = tokio::sync::oneshot::channel::<()>();
    let mut exchange = Some((request, wait));
    let mut pages = Box::pin(
        tool_pages(
            move |_| {
                let (request, wait) = exchange.take().expect("only one requested page");
                async move {
                    request.send(()).unwrap();
                    let _ = wait.await;
                    Ok(rmcp::model::ListToolsResult::default())
                }
            },
            McpDiscoveryLimits::default(),
            |_| true,
        )
        .unwrap(),
    );
    assert!(futures::poll!(pages.next()).is_pending());
    requested.await.unwrap();
    drop(pages);
    assert!(
        release.send(()).is_err(),
        "in-flight request must be dropped"
    );
}

#[tokio::test]
async fn reservations_charge_original_pages_before_filtering_and_refuse_before_more_io()
-> Result<()> {
    let mut original = page("blocked", Some("next".into()));
    original.tools.extend(page("also_blocked", None).tools);
    let last = rmcp::model::ListToolsResult::default();
    let first_bytes = u32::try_from(serde_json::to_vec(&original).unwrap().len()).unwrap();
    let total_bytes =
        first_bytes + u32::try_from(serde_json::to_vec(&last).unwrap().len()).unwrap();
    for limits in [
        McpDiscoveryLimits {
            tools: Some(1),
            ..Default::default()
        },
        McpDiscoveryLimits {
            pages: Some(1),
            ..Default::default()
        },
        McpDiscoveryLimits {
            bytes: Some(first_bytes - 1),
            ..Default::default()
        },
        McpDiscoveryLimits {
            bytes: Some(first_bytes),
            ..Default::default()
        },
        McpDiscoveryLimits {
            tools: Some(2),
            pages: Some(2),
            bytes: Some(total_bytes),
        },
    ] {
        let requests = Rc::new(RefCell::new(Vec::new()));
        let observed = requests.clone();
        let mut responses = [original.clone(), last.clone()].into_iter();
        let pages = tool_pages(
            move |cursor| {
                observed.borrow_mut().push(cursor);
                std::future::ready(Ok(responses
                    .next()
                    .expect("no request after refusal or EOF")))
            },
            limits,
            |_| false,
        )?;
        futures::pin_mut!(pages);
        if limits.tools == Some(1) || limits.bytes == Some(first_bytes - 1) {
            assert!(pages.try_next().await.is_err());
            assert_eq!(*requests.borrow(), [None]);
        } else {
            assert!(pages.try_next().await?.unwrap().is_empty());
            assert_eq!(*requests.borrow(), [None]);
            if limits.pages == Some(2) {
                assert!(pages.try_next().await?.unwrap().is_empty());
                assert_eq!(*requests.borrow(), [None, Some("next".into())]);
                assert!(pages.try_next().await?.is_none());
            } else {
                assert!(pages.try_next().await.is_err());
                assert_eq!(
                    *requests.borrow(),
                    [None],
                    "a depleted page/byte policy must not request another page"
                );
            }
        }
        assert!(pages.next().await.is_none(), "a refusal is terminal");
    }
    for limits in [
        McpDiscoveryLimits {
            tools: Some(0),
            ..Default::default()
        },
        McpDiscoveryLimits {
            pages: Some(0),
            ..Default::default()
        },
        McpDiscoveryLimits {
            bytes: Some(0),
            ..Default::default()
        },
    ] {
        let calls = Rc::new(RefCell::new(0));
        let observed = calls.clone();
        let result = tool_pages(
            move |_| {
                *observed.borrow_mut() += 1;
                std::future::ready(Err(Error::Storage("invalid policy performed I/O".into())))
            },
            limits,
            |_| true,
        );
        assert!(result.is_err());
        assert_eq!(*calls.borrow(), 0);
    }
    Ok(())
}
