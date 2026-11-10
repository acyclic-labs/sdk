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
    let pages = tool_pages(move |cursor| {
        observed.borrow_mut().push(cursor.clone());
        let page = first.take().unwrap_or_else(|| {
            let index: usize = cursor.unwrap().parse().unwrap();
            page(
                &format!("tool{index}"),
                (index < 256).then(|| (index + 1).to_string()),
            )
        });
        std::future::ready(Ok(page))
    });
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
        let pages = tool_pages(move |_| {
            let mut page = rmcp::model::ListToolsResult::default();
            page.next_cursor = cursors
                .next()
                .expect("no request after cursor error")
                .map(str::to_owned);
            std::future::ready(Ok(page))
        });
        futures::pin_mut!(pages);
        for _ in 1..expected {
            assert!(pages.try_next().await?.unwrap().is_empty());
        }
        assert!(matches!(pages.try_next().await, Err(Error::Invalid(_))));
        assert!(pages.next().await.is_none());
    }
    let pages = tool_pages(|_| std::future::ready(Ok(rmcp::model::ListToolsResult::default())));
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
    let mut pages = Box::pin(tool_pages(move |_| {
        let (request, wait) = exchange.take().expect("only one requested page");
        async move {
            request.send(()).unwrap();
            let _ = wait.await;
            Ok(rmcp::model::ListToolsResult::default())
        }
    }));
    assert!(futures::poll!(pages.next()).is_pending());
    requested.await.unwrap();
    drop(pages);
    assert!(
        release.send(()).is_err(),
        "in-flight request must be dropped"
    );
}
