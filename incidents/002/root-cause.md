# Root cause

Publication and outbox acknowledgement are separate transactions. Marking an uncertain send completed prematurely discards the durable retry obligation. Correct handling leaves pending work until a successful acknowledgement; an expired crash lease triggers retry. Relay duplicates are allowed and inbox/journal guards prevent duplicate business effects.
