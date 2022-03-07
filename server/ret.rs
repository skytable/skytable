/// Execute queries for an anonymous user
pub async fn execute_simple_noauth<'a, T: 'a + ClientConnection<Strm>, Strm: Stream>(
    db: &mut Corestore,
    con: &mut T,
    auth: &mut AuthProviderHandle<'_, T, Strm>,
    buf: SimpleQuery,
) -> crate::actions::ActionResult<()> {
    if buf.is_any_array() {
        let bufref = unsafe { buf.into_inner() };
        let mut iter = unsafe { get_iter(&bufref) };
        match iter.next_uppercase() {
            Some(auth) if ACTION_AUTH.eq(&*auth) => {
                auth::auth_login_only(db, con, auth, iter).await
            }
            Some(_) => util::err(auth::errors::AUTH_CODE_DENIED),
            None => util::err(groups::PACKET_ERR),
        }
    } else {
        util::err(groups::WRONGTYPE_ERR)
    }
}
