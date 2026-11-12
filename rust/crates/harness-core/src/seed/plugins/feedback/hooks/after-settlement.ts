export default ({ config }: { config: { retrospective: boolean } }) =>
  config.retrospective ? [{ notice: {
    wake: true,
    text: "What slowed you down or blocked you? Append feedback records; reply none if nothing.",
  } }] : [];
