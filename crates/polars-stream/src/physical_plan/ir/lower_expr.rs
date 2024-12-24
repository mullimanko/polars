use polars_core::prelude::{InitHashMaps, PlHashSet};
use polars_error::PolarsResult;
use polars_plan::plans::expr_ir::ExprIR;
use polars_plan::plans::{AExpr, IR};
use polars_utils::arena::{Arena, Node};
use polars_utils::idx_vec::UnitVec;

use crate::physical_plan::lower_expr::is_elementwise_rec_cached;
use crate::physical_plan::ExprCache;

struct LowerExprContext<'a> {
    expr_arena: &'a mut Arena<AExpr>,
    ir_arena: &'a mut Arena<IR>,
    cache: &'a mut ExprCache,
}

fn lower_exprs_with_ctx(
    ir_input: Node,
    expr_nodes: &[Node],
    ctx: &mut LowerExprContext,
) -> PolarsResult<(Node, UnitVec<Node>)> {
    let mut transformed_exprs = Vec::with_capacity(expr_nodes.len());

    // Nodes containing the columns used for executing transformed expressions.
    let mut input_irs = PlHashSet::new();

    for node in expr_nodes.iter().copied() {
        if is_elementwise_rec_cached(node, &ctx.expr_arena, ctx.cache) {
            transformed_exprs.push(node);
            continue;
        }

        match ctx.expr_arena.get(node) {
            AExpr::Explode(inner) => {
                let (transl_input, transl_exprs) = lower_exprs_with_ctx(ir_input, &[*inner], ctx)?;
                let transl_expr = transl_exprs[0];
                let transl = ctx.expr_arena.add(AExpr::Explode(transl_expr));

                input_irs.insert(transl_input);
                transformed_exprs.push(transl)
            },
            AExpr::Alias(_, _) => unreachable!("alias found in physical plan"),
            AExpr::Column(_) => unreachable!("column should always be streamable"),

            _ => {},
        }
    }

    todo!()
}

//pub fn lower_exprs(
//    root: Node,
//    exprs: &[ExprIR],
//    expr_arena: &mut Arena<AExpr>,
//    ir_arena: &mut Arena<IR>,
//    expr_cache: &mut ExprCache,
//) -> PolarsResult<(Node, Vec<ExprIR>)> {
//    let mut ctx = LowerExprContext {
//        expr_arena,
//        ir_arena,
//        cache: expr_cache,
//    };
//    let node_exprs = exprs.iter().map(|e| e.node()).collect_vec();
//    let (transformed_input, transformed_exprs) = lower_exprs_with_ctx(root, &node_exprs, &mut ctx)?;
//    let trans_expr_irs = exprs
//        .iter()
//        .zip(transformed_exprs)
//        .map(|(e, te)| ExprIR::new(te, OutputName::Alias(e.output_name().clone())))
//        .collect_vec();
//    Ok((transformed_input, trans_expr_irs))
//}
