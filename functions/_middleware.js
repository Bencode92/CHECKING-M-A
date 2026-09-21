// Protection du site par mot de passe (Basic Auth).
// Secret à définir : wrangler pages secret put SITE_PASSWORD --project-name checking-ma
export async function onRequest({ request, env, next }) {
  const expected = env.SITE_PASSWORD;
  if (!expected) return new Response("SITE_PASSWORD non configuré", { status: 500 });
  const auth = request.headers.get("Authorization") || "";
  if (auth.startsWith("Basic ")) {
    try {
      const [, pwd = ""] = atob(auth.slice(6)).split(/:(.*)/s);
      if (pwd === expected) return next();
    } catch {}
  }
  return new Response("Accès réservé", {
    status: 401,
    headers: { "WWW-Authenticate": 'Basic realm="Cibles Interim", charset="UTF-8"' },
  });
}
