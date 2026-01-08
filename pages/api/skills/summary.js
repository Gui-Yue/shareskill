import { getSkillSummary } from "../../../lib/server/skillStore";

export default async function handler(req, res) {
  if (req.method !== "GET") {
    res.setHeader("Allow", ["GET"]);
    return res.status(405).json({ error: "Method not allowed" });
  }
  try {
    const summary = await getSkillSummary();
    return res.status(200).json(summary);
  } catch (error) {
    return res.status(500).json({ error: error?.message || "Failed to load summary" });
  }
}
