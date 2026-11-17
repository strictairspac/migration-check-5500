import {batches,mappings,runs,revocations} from '../api/business.js';
export const store={batches:[],mappings:[],runs:[],revocations:[],user:null};
export async function refresh(){[store.batches,store.mappings,store.runs,store.revocations]=await Promise.all([batches.list(),mappings.list(),runs.list(),revocations.list()]);}
