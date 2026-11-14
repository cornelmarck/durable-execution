import { Link } from "react-router";
import { Empty } from "../components/ui";

export function NotFound() {
  return (
    <Empty>
      Nothing here. <Link to="/">Back to the overview</Link>
    </Empty>
  );
}
