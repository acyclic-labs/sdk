// The disabled production macro must discard its argument before name resolution.
const _: () = {
    disabled::record!(scan(unavailable_argument()));
};
